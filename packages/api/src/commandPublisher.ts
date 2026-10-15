/**
 * Publishes a gateway's desired mode: retained, QoS 1, on `homes/{id}/commands`. The API connects with
 * its own certificate (CN `api`), which the broker ACL allows to write only command topics (S4-1).
 * Retained means a gateway that's offline now gets the latest command when it next connects. The retained message is
 * a copy of `gateways.desired_mode`, which the reconciler rebuilds (S7-1), so it isn't the only record.
 */
import { connect } from "mqtt";
import type { MqttClient } from "mqtt";
import { commandTopic } from "@fleet-ingest/contract";
import type { GatewayMode } from "@fleet-ingest/contract";

/** What the API needs from the broker. */
export interface CommandPublisher {
  /** Publishes the mode retained at QoS 1, and resolves once the broker has acknowledged it. Rejects if it refused. */
  publishMode(gatewayId: string, mode: GatewayMode): Promise<void>;
  /** True while connected to the broker; part of `/health/ready`. */
  connected(): boolean;
  /** Calls `listener` on every connection to the broker, the first one included if it's registered before it. */
  onConnect(listener: () => void): void;
  close(): Promise<void>;
}

/** Connection settings for the publisher. */
export interface PublisherOptions {
  url: string;
  clientId: string;
  tls?: { ca: Buffer; cert: Buffer; key: Buffer };
}

/** Connects to the broker; it reconnects on its own. Resolves once the first connection is up. */
export async function startCommandPublisher(options: PublisherOptions): Promise<CommandPublisher> {
  const client: MqttClient = connect(options.url, {
    clientId: options.clientId,
    protocolVersion: 5,
    clean: true,
    reconnectPeriod: 1000,
    ...(options.tls ? { ...options.tls, rejectUnauthorized: true } : {}),
  });
  await new Promise<void>((resolve, reject) => {
    client.once("connect", () => resolve());
    client.once("error", reject);
  });
  return {
    publishMode: async (gatewayId, mode) => {
      await client.publishAsync(commandTopic(gatewayId), JSON.stringify({ mode }), { qos: 1, retain: true });
    },
    connected: () => client.connected,
    onConnect: (listener) => void client.on("connect", listener),
    close: async () => void (await client.endAsync()),
  };
}
