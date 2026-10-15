/**
 * One virtual gateway on the wire: its own MQTT connection, a retained `offline` Last Will, a retained `online` status
 * on connect and every status interval, the desired mode from its retained command topic (reported back in its
 * status), and an uplink loop that empties the buffer the way the real gateway does
 * (`MqttUplinkService.cs`):
 * - at most `window` publishes waiting for PUBACK at once;
 * - an even pace of at most `replayPerSecond`;
 * - a message leaves the buffer only when the broker has acknowledged it.
 */
import { connect } from "mqtt";
import type { MqttClient } from "mqtt";
import { commandTopic, gatewayCommandSchema, statusTopic, telemetryTopic } from "@fleet-ingest/contract";
import type { GatewayStatus, TelemetryMessage } from "@fleet-ingest/contract";
import type { AckLedger } from "../core/ackLedger.ts";
import type { RawMessage } from "../core/chaos.ts";
import { EvenPace } from "../core/rateLimiter.ts";
import { TelemetryBuffer } from "../core/telemetryBuffer.ts";

/** Uplink settings, defaulting to the real gateway's `UplinkOptions`. */
export interface UplinkSettings {
  brokerUrl: string;
  window: number;
  replayPerSecond: number;
  bufferCapacity: number;
  statusIntervalMs: number;
  mode: string;
  /** PEM contents for mutual TLS (D6): the lab CA, and this gateway's certificate (CN = gateway ID) and key. */
  tls?: { ca: Buffer; cert: Buffer; key: Buffer };
  /** The name the broker's certificate carries, when connecting through a proxy whose host name differs (toxiproxy). */
  serverName?: string;
  /** MQTT keepalive; the real gateway uses 15 s, so the broker publishes its Last Will about 22 s after a link dies. */
  keepaliveSeconds: number;
  /**
   * How long one connection attempt may wait for CONNACK before it's abandoned and retried. The real gateway gives
   * MQTTnet 10 s (`MqttUplinkService.cs`, `WithTimeout`); mqtt.js's own default is 30 s. Behind a proxy or load balancer
   * that accepts TCP while the broker is down, every attempt made during a restart waits this long (S7-8).
   */
  connectTimeoutMs: number;
  /**
   * How long to wait before each reconnect attempt. With `reconnectJitterMaxMs` > 0, a uniformly random time in
   * [0, max) (full jitter), so a fleet coming back from a broker outage spreads out (scenario 4). With 0, a fixed 2 s:
   * every gateway retries in step, which is the no-jitter worst case.
   */
  reconnectJitterMaxMs: number;
  random: () => number;
}

/** A running virtual gateway. */
export interface VirtualGateway {
  gatewayId: string;
  /** Stores a reading in the outbox; the uplink loop sends it when it can. */
  record: (message: TelemetryMessage) => void;
  /** Readings waiting in the outbox, not yet acknowledged by the broker. */
  buffered: () => number;
  /** Readings dropped because the outbox was full (oldest first), reported as `droppedTelemetry`. */
  dropped: () => number;
  /**
   * Publishes a raw telemetry message at QoS 1, bypassing the outbox and the ledger: poison for scenario 5. If the
   * gateway isn't connected, it's skipped; poison isn't data, so nothing is buffered or retried.
   */
  publishRaw: (message: RawMessage) => void;
  /** Disconnects politely: the gateway publishes `offline` itself, so the Last Will isn't needed. */
  stop: () => Promise<void>;
}

/** Resolves after `ms` milliseconds. */
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Starts a virtual gateway. Connection loss is normal: mqtt.js reconnects, and the buffer holds readings meanwhile. */
export function startVirtualGateway(gatewayId: string, settings: UplinkSettings, ledger: AckLedger, onError: (error: Error) => void): VirtualGateway {
  const buffer = new TelemetryBuffer<TelemetryMessage>(settings.bufferCapacity);
  const pace = new EvenPace(settings.replayPerSecond);
  let stopped = false;
  let wake: (() => void) | undefined;

  const client = connect(settings.brokerUrl, {
    clientId: gatewayId,
    protocolVersion: 5,
    clean: true,
    reconnectPeriod: reconnectDelayMs(settings),
    keepalive: settings.keepaliveSeconds,
    connectTimeout: settings.connectTimeoutMs,
    ...(settings.tls ? { ...settings.tls, rejectUnauthorized: true } : {}),
    ...(settings.serverName ? { servername: settings.serverName } : {}),
    will: { topic: statusTopic(gatewayId), payload: Buffer.from(JSON.stringify({ state: "offline" })), qos: 1, retain: true },
  });
  client.on("error", onError);
  for (const event of ["close", "reconnect", "connect"] as const) client.on(event, () => void (client.options.reconnectPeriod = reconnectDelayMs(settings)));

  client.on("connect", () => {
    publishOnline(client, gatewayId, buffer, settings.mode, ledger);
    void client.subscribeAsync(commandTopic(gatewayId), { qos: 1 }).catch(onError);
    wake?.();
  });
  client.on("message", (topic, payload) => {
    const mode = topic === commandTopic(gatewayId) ? parseMode(payload) : null;
    if (mode && mode !== settings.mode) {
      settings.mode = mode;
      publishOnline(client, gatewayId, buffer, settings.mode, ledger);
    }
  });
  const statusTimer = setInterval(() => client.connected && publishOnline(client, gatewayId, buffer, settings.mode, ledger), settings.statusIntervalMs);

  void runUplink(client, buffer, pace, settings.window, ledger, () => stopped, (resolve) => (wake = resolve)).catch(onError);

  return {
    gatewayId,
    record: (message) => {
      buffer.push(message);
      wake?.();
    },
    buffered: () => buffer.size(),
    dropped: () => buffer.dropped(),
    publishRaw: (message) => {
      if (client.connected) void client.publishAsync(telemetryTopic(gatewayId, message.topicDeviceId), message.payload, { qos: 1 }).catch(onError);
    },
    stop: async () => {
      stopped = true;
      clearInterval(statusTimer);
      wake?.();
      if (client.connected) {
        await client.publishAsync(statusTopic(gatewayId), JSON.stringify({ state: "offline", timestamp: new Date().toISOString() }), { qos: 1, retain: true });
      }
      await client.endAsync();
    },
  };
}

/**
 * The wait before the next reconnect attempt: full jitter up to the configured maximum, or a fixed 2 s. mqtt.js
 * keeps reconnecting by itself and reads `options.reconnectPeriod` each time it schedules the next attempt, so a
 * fresh value is drawn on every close, reconnect and connect. Turning mqtt.js's reconnect off and reconnecting by
 * hand made it flush its outgoing queue: the uplink then waited forever on publishes that were never resolved (S6-9).
 */
function reconnectDelayMs(settings: UplinkSettings): number {
  return settings.reconnectJitterMaxMs > 0 ? Math.floor(settings.random() * settings.reconnectJitterMaxMs) : 2000;
}

/** The mode in a command payload, or null if it isn't a valid command (it's then ignored, like the real gateway). */
function parseMode(payload: Buffer): string | null {
  try {
    const parsed = gatewayCommandSchema.safeParse(JSON.parse(payload.toString()));
    return parsed.success ? parsed.data.mode : null;
  } catch {
    return null;
  }
}

/**
 * Publishes the retained `online` status with the outbox counters, as the real gateway does on connect and every 60 s.
 * When the broker acknowledges it, it's recorded in the ledger.
 */
function publishOnline(client: MqttClient, gatewayId: string, buffer: TelemetryBuffer<TelemetryMessage>, mode: string, ledger: AckLedger): void {
  const status: GatewayStatus = {
    state: "online", timestamp: new Date().toISOString(), bufferedTelemetry: buffer.size(), droppedTelemetry: buffer.dropped(), mode,
  };
  client.publishAsync(statusTopic(gatewayId), JSON.stringify(status), { qos: 1, retain: true }).then(() => ledger.recordStatus(gatewayId), () => undefined);
}

/**
 * The uplink loop. While connected, it takes the oldest buffered reading, waits for its pace slot, and publishes it,
 * keeping up to `window` publishes in flight. When the window is full it waits for the oldest PUBACK. If the
 * connection drops, everything still in flight goes back to the front of the buffer, so it's resent in order.
 * Between messages it sleeps until woken by a new reading or a reconnect. Every PUBACK is recorded in the ledger.
 */
async function runUplink(
  client: MqttClient,
  buffer: TelemetryBuffer<TelemetryMessage>,
  pace: EvenPace,
  window: number,
  ledger: AckLedger,
  isStopped: () => boolean,
  setWake: (resolve: () => void) => void,
): Promise<void> {
  const inFlight: Array<{ message: TelemetryMessage; ack: Promise<unknown> }> = [];
  while (!isStopped()) {
    const next = client.connected ? buffer.shift() : undefined;
    if (!next) {
      await settle(inFlight, buffer);
      await new Promise<void>((resolve) => {
        setWake(resolve);
        setTimeout(resolve, 1000);
      });
      continue;
    }
    const wait = pace.claim(Date.now());
    if (wait > 0) await sleep(wait);
    const ack = client.publishAsync(telemetryTopic(clientGatewayId(client), next.deviceId), JSON.stringify(next), { qos: 1 })
      .then(() => ledger.record(next.deviceId, next.epoch, next.seq));
    inFlight.push({ message: next, ack });
    if (inFlight.length >= window) await settleOldest(inFlight, buffer);
  }
}

/**
 * Waits for the oldest in-flight publish. If it failed, the connection is gone and the rest will fail too, so it
 * settles them all and returns every failed message to the buffer together, which keeps the original order.
 */
async function settleOldest(inFlight: Array<{ message: TelemetryMessage; ack: Promise<unknown> }>, buffer: TelemetryBuffer<TelemetryMessage>): Promise<void> {
  const [oldest] = await Promise.allSettled([inFlight[0]?.ack]);
  if (oldest?.status === "fulfilled") inFlight.shift();
  else await settle(inFlight, buffer);
}

/** Waits for the given in-flight publishes; any that failed go back to the front of the buffer, in order. */
async function settle(inFlight: Array<{ message: TelemetryMessage; ack: Promise<unknown> }>, buffer: TelemetryBuffer<TelemetryMessage>): Promise<void> {
  const taken = inFlight.splice(0, inFlight.length);
  const results = await Promise.allSettled(taken.map((entry) => entry.ack));
  const failed = taken.filter((_, i) => results[i]?.status === "rejected").map((entry) => entry.message);
  if (failed.length) buffer.returnToFront(failed);
}

/** The gateway ID is the MQTT client ID (one certificate per gateway in slice 4, CN = gateway ID). */
function clientGatewayId(client: MqttClient): string {
  return client.options.clientId as string;
}
