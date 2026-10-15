/**
 * MQTT clients for the spike: an ingest-like subscriber that holds its PUBACKs until told to release them,
 * a QoS 1 publisher that sends numbered readings, and a reader for broker `$SYS` values.
 *
 * The subscriber has two ways of holding acks, because the spike compares them:
 * - "documented": hold the callback that mqtt.js passes to `customHandleAcks`. This is the documented API.
 * - "deferred": call that callback at once, but intercept the PUBACK packet on its way out and hold it.
 */
import { connect } from "mqtt";
import type { IClientOptions, MqttClient, Packet } from "mqtt";

/** One message as the subscriber saw it. */
export interface Received {
  seq: number;
  dup: boolean;
  topic: string;
  at: number;
}

/** How the subscriber holds acknowledgements; see the module header. */
export type AckMode = "documented" | "deferred";

/** Options for {@link connectIngest}. */
export interface IngestOptions {
  url: string;
  clientId: string;
  topic: string;
  receiveMaximum: number;
  mode: AckMode;
  sessionExpirySeconds?: number;
  keepaliveSeconds?: number;
  autoAck?: boolean;
}

/** A connected ingest-like subscriber and the controls a check needs. */
export interface Ingest {
  client: MqttClient;
  received: Received[];
  /** Number of messages received whose PUBACK hasn't been sent yet. */
  pending(): number;
  /** Sends the PUBACKs for the oldest `count` held messages, in arrival order. */
  release(count?: number): void;
  /** Drops the TCP connection without a DISCONNECT packet, like `kill -9` on the process. */
  kill(): void;
  /** Disconnects politely. The persistent session stays on the broker. */
  close(): Promise<void>;
}

/** Parses the `seq` from a spike payload. */
function seqOf(payload: Buffer): number {
  return (JSON.parse(payload.toString()) as { seq: number }).seq;
}

/**
 * Connects an MQTT v5 subscriber with a persistent session (`clean: false`, session expiry, its own Receive Maximum)
 * and subscribes at QoS 1. It never auto-reconnects, so a check controls exactly when a session resumes.
 * With `autoAck`, every message is acknowledged as soon as it arrives.
 */
export async function connectIngest(options: IngestOptions): Promise<Ingest> {
  const received: Received[] = [];
  const held: Array<() => void> = [];
  const clientOptions: IClientOptions = {
    clientId: options.clientId,
    protocolVersion: 5,
    clean: false,
    reconnectPeriod: 0,
    keepalive: options.keepaliveSeconds ?? 30,
    properties: {
      sessionExpiryInterval: options.sessionExpirySeconds ?? 3600,
      receiveMaximum: options.receiveMaximum,
    },
    customHandleAcks: (topic, message, packet, done) => {
      received.push({ seq: seqOf(message), dup: packet.dup, topic, at: Date.now() });
      if (options.autoAck || options.mode === "deferred") done(0);
      else held.push(() => done(0));
    },
  };
  const client = connect(options.url, clientOptions);
  if (options.mode === "deferred" && !options.autoAck) holdOutgoingPubacks(client, held);
  await new Promise<void>((resolve, reject) => {
    client.once("connect", () => resolve());
    client.once("error", reject);
  });
  await client.subscribeAsync(options.topic, { qos: 1 });
  return {
    client,
    received,
    pending: () => held.length,
    release: (count = held.length) => {
      for (const send of held.splice(0, count)) send();
    },
    kill: () => {
      client.end(true);
    },
    close: async () => {
      await client.endAsync();
    },
  };
}

/**
 * Patches the client's internal packet writer so outgoing PUBACKs are queued in `held`, in arrival order,
 * instead of being written. mqtt.js gets its write callback straight away, so it carries on reading the next packet.
 * This touches the private `_sendPacket`, which is acceptable in a spike pinned to mqtt.js 5.16.0. Slice 3 decides
 * whether production ingest may do the same.
 */
function holdOutgoingPubacks(client: MqttClient, held: Array<() => void>): void {
  const target = client as unknown as { _sendPacket: (packet: Packet, cb?: () => void) => void };
  const original = target._sendPacket.bind(client);
  target._sendPacket = (packet, cb) => {
    if (packet.cmd !== "puback") return original(packet, cb);
    held.push(() => original(packet));
    cb?.();
  };
}

/** A realistic telemetry payload of about 300 bytes, carrying the spike's sequence number. */
export function telemetryPayload(seq: number): string {
  return JSON.stringify({
    deviceId: "dev-01", epoch: 1, seq, timestamp: new Date().toISOString(), kind: "Inverter",
    pvPowerW: 3120.5, batteryPowerW: -850.25, gridPowerW: 412.75, evPowerW: 0, energyWh: 1834567.5,
    socPercent: 64.5, chargeLimitA: 16, note: "spike-padding-to-reach-a-realistic-size-0123456789abcdef",
  });
}

/**
 * Publishes readings `from`..`to` (inclusive) at QoS 1, with up to `window` publishes waiting for PUBACK at once,
 * then disconnects. Resolves once the broker has acknowledged every one.
 */
export async function publishSequence(url: string, topic: string, from: number, to: number, window = 100): Promise<void> {
  const client = await connectPublisher(url);
  let next = from;
  const worker = async (): Promise<void> => {
    while (next <= to) {
      const seq = next++;
      await client.publishAsync(topic, telemetryPayload(seq), { qos: 1 });
    }
  };
  await Promise.all(Array.from({ length: window }, worker));
  await client.endAsync();
}

/** Connects a plain v5 publisher with a clean session. */
export async function connectPublisher(url: string, clientId = `pub-${process.pid}-${Date.now()}`): Promise<MqttClient> {
  const client = connect(url, { clientId, protocolVersion: 5, clean: true, reconnectPeriod: 0 });
  await new Promise<void>((resolve, reject) => {
    client.once("connect", () => resolve());
    client.once("error", reject);
  });
  return client;
}

/**
 * Collects every `$SYS` topic and its latest value for `ms` milliseconds. `$SYS` is published every `sys_interval`
 * (1 s in the spike), so a few seconds is enough to see every topic at least once.
 */
export async function readSys(url: string, ms = 2500): Promise<Map<string, string>> {
  const client = await connectPublisher(url, `sys-${Date.now()}`);
  const values = new Map<string, string>();
  client.on("message", (topic, payload) => values.set(topic, payload.toString()));
  await client.subscribeAsync("$SYS/#", { qos: 0 });
  await new Promise((resolve) => setTimeout(resolve, ms));
  await client.endAsync();
  return values;
}

/** Reads one numeric `$SYS` value, waiting up to a few seconds for it to be published. Returns NaN if it never is. */
export async function readSysNumber(url: string, topic: string): Promise<number> {
  const values = await readSys(url);
  return Number(values.get(topic) ?? NaN);
}

/** Resolves when `predicate` turns true, polling every 20 ms; rejects with `label` after `timeoutMs`. */
export async function waitUntil(predicate: () => boolean, timeoutMs: number, label: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${label}`);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

