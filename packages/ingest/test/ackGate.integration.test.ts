/**
 * Integration tests for the AckGate against a real Mosquitto 2.1.2 broker.
 * - The canary: fails if a mqtt.js upgrade stops PUBACKs being held, or makes delivery serialise again (S0-1).
 * - The connection-generation rule, plus its deliberate-break check: with the rule switched off, a stale ack written
 *   on a new connection silently loses messages, and the same scenario detects it.
 */
import { connect } from "mqtt";
import type { MqttClient } from "mqtt";
import type { StartedTestContainer } from "testcontainers";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { telemetryTopic } from "@fleet-ingest/contract";
import { AckGate } from "../src/adapters/ackGate.ts";
import type { AckToken } from "../src/adapters/ackGate.ts";
import { startSubscriber } from "../src/adapters/mqttSubscriber.ts";
import type { Delivery, Subscriber } from "../src/adapters/mqttSubscriber.ts";
import { startMosquitto } from "./support/containers.ts";

let broker: { container: StartedTestContainer; url: string };
let clientCounter = 0;

/**
 * A fresh broker per test: every test's persistent session joins the same shared-subscription group, and a session
 * left behind by an earlier test would take its round-robin share of the next test's messages.
 */
beforeEach(async () => {
  broker = await startMosquitto();
});

afterEach(async () => {
  await broker?.container.stop();
});

/** Resolves when `predicate` is true, polling; fails the test after `ms`. */
async function waitFor(predicate: () => boolean, ms = 10_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

/** Resolves after `ms` milliseconds. */
const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Publishes `count` QoS 1 readings for one device and waits for the broker's PUBACKs. */
async function publish(count: number, from = 1): Promise<void> {
  const client = connect(broker.url, { protocolVersion: 5, clientId: `pub-${clientCounter++}` });
  await new Promise((resolve) => client.once("connect", resolve));
  for (let seq = from; seq < from + count; seq++) {
    const payload = JSON.stringify({ deviceId: "dev-1", epoch: 1, seq, timestamp: new Date().toISOString(), kind: "Inverter" });
    await client.publishAsync(telemetryTopic("gw-1", "dev-1"), payload, { qos: 1 });
  }
  await client.endAsync();
}

/** Starts the real subscriber with its own client ID and collects deliveries without acknowledging them. */
async function subscribe(clientId: string, receiveMaximum: number, createGate?: typeof defaultGate): Promise<{ sub: Subscriber; deliveries: Delivery[] }> {
  const deliveries: Delivery[] = [];
  const sub = await startSubscriber({
    url: broker.url, clientId, receiveMaximum, sessionExpirySeconds: 600, keepaliveSeconds: 30, reconnectPeriodMs: 250,
    now: () => new Date(), onDelivery: (d) => deliveries.push(d), ...(createGate ? { createGate } : {}),
  });
  return { sub, deliveries };
}

/** The production gate factory, for symmetry with the broken one. */
const defaultGate = (client: MqttClient) => new AckGate(client);

/** Reconnects a session with a plain auto-acking client and counts what the broker still had for it. */
async function countRedelivered(clientId: string): Promise<number> {
  const received: number[] = [];
  const client = connect(broker.url, {
    protocolVersion: 5, clientId, clean: false, properties: { sessionExpiryInterval: 600 }, reconnectPeriod: 0,
  });
  client.on("message", (_topic, payload) => received.push(JSON.parse(payload.toString()).seq));
  await new Promise((resolve) => client.once("connect", resolve));
  await pause(1500);
  await client.endAsync();
  return received.length;
}

describe("AckGate canary (fails if a mqtt.js upgrade changes the interception)", () => {
  it("receives a whole batch while every PUBACK is held, instead of one at a time", async () => {
    const { sub, deliveries } = await subscribe("ingest-canary-batch", 100);
    await publish(20);
    await waitFor(() => deliveries.length === 20);
    expect(sub.gate.outstanding()).toBe(20);
    deliveries.forEach((d) => d.ack());
    await pause(300);
    await sub.close();
    expect(await countRedelivered("ingest-canary-batch")).toBe(0);
  });

  it("really holds the PUBACKs: the broker stops at Receive Maximum until acks are released", async () => {
    const { sub, deliveries } = await subscribe("ingest-canary-held", 10);
    await publish(30);
    await pause(1000);
    expect(deliveries.length).toBe(10);
    deliveries.slice(0, 5).forEach((d) => d.ack());
    await waitFor(() => deliveries.length === 15);
    await pause(500);
    expect(deliveries.length).toBe(15);
    deliveries.slice(5).forEach((d) => d.ack());
    await sub.close();
  });
});

/** A gate with the connection-generation rule switched off: the deliberate break. */
class GateWithoutGenerations extends AckGate {
  protected override isStale(_token: AckToken): boolean {
    return false;
  }
}

/**
 * The scenario: 5 messages arrive and are held. The connection drops, and on resume the broker redelivers the same 5
 * on the new connection. Now the old connection's processing finishes and its tokens are released. The new
 * connection's work never finishes, because the process dies. Afterwards, how many of the 5 does the broker still
 * have for us?
 */
async function staleAckScenario(clientId: string, createGate: typeof defaultGate): Promise<number> {
  const { sub, deliveries } = await subscribe(clientId, 100, createGate);
  await publish(5, 100);
  await waitFor(() => deliveries.length === 5);
  sub.client.stream.destroy();
  await waitFor(() => deliveries.length === 10);
  deliveries.slice(0, 5).forEach((d) => d.ack());
  await pause(300);
  sub.client.end(true);
  return countRedelivered(clientId);
}

describe("connection-generation rule", () => {
  it("discards acks from a closed connection, so nothing uncommitted is acknowledged: all 5 come back", async () => {
    expect(await staleAckScenario("ingest-gen-safe", defaultGate)).toBe(5);
  });

  it("deliberate break: without the rule, the stale acks acknowledge the redeliveries and all 5 are lost", async () => {
    expect(await staleAckScenario("ingest-gen-broken", (client) => new GateWithoutGenerations(client))).toBe(0);
  });
});
