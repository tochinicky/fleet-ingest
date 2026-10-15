/**
 * Integration test for decisions.md S3-9: after a clean broker restart, Mosquitto 2.1.2 restores the ingest session
 * without its shared subscriptions but reports "session present". The subscriber must subscribe again on every
 * connect, or it sits connected and receives nothing.
 */
import { connect } from "mqtt";
import type { StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { telemetryTopic } from "@fleet-ingest/contract";
import { startSubscriber } from "../src/adapters/mqttSubscriber.ts";
import type { Delivery } from "../src/adapters/mqttSubscriber.ts";
import { startMosquitto } from "./support/containers.ts";

let broker: { container: StartedTestContainer; url: string };

beforeAll(async () => {
  broker = await startMosquitto(["persistence true", "persistence_location /mosquitto/data/"]);
});

afterAll(async () => {
  await broker?.container.stop();
});

/** Resolves when `predicate` is true, polling; fails after `ms`. */
async function waitFor(predicate: () => boolean, ms = 15_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

/** Publishes one QoS 1 reading with the given seq. */
async function publish(seq: number): Promise<void> {
  const client = connect(broker.url, { protocolVersion: 5, clientId: `pub-${seq}-${Date.now()}`, reconnectPeriod: 0 });
  await new Promise((resolve) => client.once("connect", resolve));
  await client.publishAsync(telemetryTopic("gw-1", "dev-1"), JSON.stringify({ deviceId: "dev-1", epoch: 1, seq, timestamp: new Date().toISOString(), kind: "Inverter" }), { qos: 1 });
  await client.endAsync();
}

describe("broker restart", () => {
  it("keeps receiving after a clean broker restart, because the subscriber subscribes again on reconnect", async () => {
    const deliveries: Delivery[] = [];
    const sub = await startSubscriber({
      url: broker.url, clientId: "ingest-restart", receiveMaximum: 100, sessionExpirySeconds: 600, keepaliveSeconds: 30, reconnectPeriodMs: 250,
      now: () => new Date(), onDelivery: (d) => { deliveries.push(d); d.ack(); },
    });
    await publish(1);
    await waitFor(() => deliveries.length === 1);
    let reconnected = false;
    sub.client.on("connect", () => (reconnected = true));
    await broker.container.restart();
    broker.url = `mqtt://${broker.container.getHost()}:${broker.container.getMappedPort(1883)}`;
    sub.client.options.port = broker.container.getMappedPort(1883);
    await waitFor(() => reconnected, 30_000);
    await new Promise((resolve) => setTimeout(resolve, 500));
    await publish(2);
    await waitFor(() => deliveries.length === 2);
    expect(deliveries.map((d) => JSON.parse(Buffer.from(d.payload).toString()).seq)).toEqual([1, 2]);
    await sub.close();
  });
});
