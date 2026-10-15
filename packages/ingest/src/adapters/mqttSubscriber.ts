/**
 * The MQTT side of ingest: one MQTT v5 connection with a persistent session that joins the shared subscriptions (D2),
 * and hands every message to the pipeline as a `Delivery` that is acknowledged only when the pipeline says so (D3).
 */
import { connect } from "mqtt";
import type { MqttClient } from "mqtt";
import { SHARED_STATUS_SUBSCRIPTION, SHARED_TELEMETRY_SUBSCRIPTION } from "@fleet-ingest/contract";
import { AckGate } from "./ackGate.ts";
import type { StaleAckListener } from "./ackGate.ts";

/** One received message and the only way to acknowledge it. */
export interface Delivery {
  topic: string;
  payload: Uint8Array;
  receivedAt: Date;
  /** True when the broker is resending it (an earlier delivery was never acknowledged). */
  redelivered: boolean;
  /** Acknowledges the message. Call only after it's stored; PUBACKs still go out in arrival order. */
  ack: () => void;
}

/** Connection settings. `clientId` must be stable per replica, so a restart resumes its session (D2). */
export interface SubscriberOptions {
  url: string;
  clientId: string;
  receiveMaximum: number;
  sessionExpirySeconds: number;
  keepaliveSeconds: number;
  /**
   * How soon to retry a lost broker connection. Kept well below the gateways' first retry (1 s), so after a broker
   * restart ingest has subscribed again before gateways start sending. With persist-sqlite the broker keeps the shared
   * subscriptions anyway; this still matters on a brand-new broker and on the snapshot fallback (S3-9, S7-7).
   */
  reconnectPeriodMs: number;
  /** The clock that stamps `receivedAt`, injected so tests can control it. */
  now: () => Date;
  /** Called once per received message, in arrival order. The handler must call `ack` only after the message is stored. */
  onDelivery: (delivery: Delivery) => void;
  onStaleAck?: StaleAckListener;
  /** PEM contents for mutual TLS (D6): the CA that signed the broker, and this replica's certificate and key. */
  tls?: { ca: Buffer; cert: Buffer; key: Buffer };
  /**
   * Called for every connection lifecycle event (connect, close, offline, error, packet-level connack), with a short
   * detail. Used to log reconnects with timestamps; decisions.md S4-6 traces a slow restart with it.
   */
  onConnectionEvent?: (event: string, detail?: string) => void;
  /** Called after each connect once both subscriptions are acknowledged, with the time since CONNECT was sent. */
  onSubscribed?: (connectToSubackMs: number) => void;
  /** Called if re-subscribing after a reconnect fails; the caller should treat it as fatal. */
  onSubscribeError?: (error: unknown) => void;
  /** Builds the ack gate. Only tests pass this, to run the same scenarios with a deliberately broken gate. */
  createGate?: (client: MqttClient, onStale?: StaleAckListener) => AckGate;
}

/** A running subscriber. */
export interface Subscriber {
  client: MqttClient;
  gate: AckGate;
  /** Disconnects politely. The session stays on the broker, so anything unacknowledged is redelivered later. */
  close: () => Promise<void>;
}

/**
 * Connects and subscribes. mqtt.js reconnects on its own; every reconnect resumes the same session, starts a new ack
 * generation, and subscribes again. The returned promise resolves after the first connection is up and both
 * subscriptions are in place.
 */
export async function startSubscriber(options: SubscriberOptions): Promise<Subscriber> {
  let gate: AckGate | undefined;
  const client = connect(options.url, {
    clientId: options.clientId,
    protocolVersion: 5,
    clean: false,
    keepalive: options.keepaliveSeconds,
    reconnectPeriod: options.reconnectPeriodMs,
    resubscribe: false,
    manualConnect: true,
    properties: { sessionExpiryInterval: options.sessionExpirySeconds, receiveMaximum: options.receiveMaximum },
    ...(options.tls ? { ...options.tls, rejectUnauthorized: true } : {}),
    customHandleAcks: (topic, message, packet, done) => {
      const token = (gate as AckGate).register(packet.messageId ?? 0);
      done(0);
      options.onDelivery({
        topic,
        payload: message,
        receivedAt: options.now(),
        redelivered: packet.dup,
        ack: () => (gate as AckGate).release(token),
      });
    },
  });
  gate = (options.createGate ?? ((c, onStale) => new AckGate(c, onStale)))(client, options.onStaleAck);
  let connectSentAt = Date.now();
  client.on("packetsend", (packet) => {
    if (packet.cmd === "connect") connectSentAt = Date.now();
  });
  const firstSubscription = new Promise<void>((resolve, reject) => {
    client.on("connect", () => void subscribe(client).then(() => {
      options.onSubscribed?.(Date.now() - connectSentAt);
      resolve();
    }, (error) => (options.onSubscribeError ?? reject)(error)));
    client.once("error", reject);
  });
  reportConnectionEvents(client, options.onConnectionEvent);
  client.connect();
  await firstSubscription;
  return { client, gate, close: async () => void (await client.endAsync()) };
}

/** Forwards the client's connection lifecycle events to `report`, if one is given. */
function reportConnectionEvents(client: MqttClient, report: SubscriberOptions["onConnectionEvent"]): void {
  if (!report) return;
  client.on("connect", (connack) => report("connect", `sessionPresent=${connack.sessionPresent}`));
  client.on("close", () => report("close"));
  client.on("offline", () => report("offline"));
  client.on("error", (error) => report("error", error.message));
  client.on("packetsend", (packet) => packet.cmd === "connect" && report("connect-sent"));
}

/**
 * Joins both shared subscriptions. Called on **every** connect, even when the broker reports the session as present:
 * with snapshot persistence (the documented fallback), Mosquitto 2.1.2 restores a persistent session's plain
 * subscriptions after a restart but not its shared ones, while still saying "session present" (decisions.md S3-9).
 * persist-sqlite, the current back end, restores them (S6-12); subscribing again is kept for the fallback and a
 * brand-new broker. mqtt.js only re-subscribes when the session is *not* present,
 * so without this, ingest would sit connected and receive nothing. Subscribing again to an existing subscription
 * just replaces it, so this is always safe.
 */
async function subscribe(client: MqttClient): Promise<void> {
  await client.subscribeAsync([SHARED_TELEMETRY_SUBSCRIPTION, SHARED_STATUS_SUBSCRIPTION], { qos: 1 });
}
