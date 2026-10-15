/**
 * Canaries for the persist-sqlite plugin on the pinned 2.1.2 image (decisions.md S6-12, S6-14). Each one pins an open
 * upstream bug as it affects fleet-ingest, so an image bump that changes the behaviour fails here first:
 * - #3758: an in-flight message restored after a restart comes back without DUP. Harmless for storage, because the
 *   primary key absorbs the copy; only the `redelivered` label of the received counter undercounts.
 * - #3761: a session ended inside the uncommitted flush window comes back after a crash. For a shared-subscription
 *   member, that's a ghost that takes a round-robin share of readings. Waiting one flush period after ending it is safe.
 * - No snapshot migration: switching an existing `mosquitto.db` volume to persist-sqlite starts with no sessions and
 *   no queue, so the switch is a fresh install, made with an empty broker queue.
 * - A live reconnect (no broker restart) redelivers unacknowledged messages with DUP set, on both back ends: what
 *   the `redelivered` label of ingest's received counter means (S7-7).
 * Each test runs its own brokers on a host data directory, so a "restart" is a new container on the same files.
 */
import { execFileSync } from "node:child_process";
import { chmodSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect } from "mqtt";
import type { MqttClient } from "mqtt";
import { GenericContainer, Wait } from "testcontainers";
import type { StartedTestContainer } from "testcontainers";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { telemetryTopic } from "@fleet-ingest/contract";
import { startSubscriber } from "../src/adapters/mqttSubscriber.ts";
import type { Delivery } from "../src/adapters/mqttSubscriber.ts";
import { MOSQUITTO_IMAGE } from "./support/containers.ts";

/** The shared-subscription filter ingest uses. */
const SHARED = "$share/ingest/homes/+/devices/+/telemetry";

/** The plugin's commit interval, as the deploy config sets it. */
const FLUSH_PERIOD_MS = 5_000;

/** Lines every canary broker shares, before its persistence lines. */
const BASE = ["listener 1883", "allow_anonymous true", "persistence_location /mosquitto/data/", "log_dest stdout"];

/** The two persistence back ends: snapshots (the current deploy) and the persist-sqlite plugin. */
const PERSISTENCE = {
  snapshot: ["persistence true", "autosave_interval 60"],
  sqlite: ["global_plugin /usr/lib/mosquitto_persist_sqlite.so", `plugin_opt_flush_period ${FLUSH_PERIOD_MS / 1000}`],
};

let dataDir: string;
let containers: StartedTestContainer[] = [];
let clientCounter = 0;

beforeEach(() => {
  dataDir = mkdtempSync(join(tmpdir(), "persist-sqlite-"));
  chmodSync(dataDir, 0o777);
});

afterEach(async () => {
  for (const container of containers) await container.stop().catch(() => undefined);
  containers = [];
  rmSync(dataDir, { recursive: true, force: true });
});

/**
 * Starts a broker on the test's data directory and returns its `mqtt://` URL once a client can connect. Docker
 * Desktop's port forward can refuse the first connection after the broker logs "running".
 */
async function startBroker(persistence: keyof typeof PERSISTENCE): Promise<string> {
  const config = [...BASE, ...PERSISTENCE[persistence], ""].join("\n");
  const container = await new GenericContainer(MOSQUITTO_IMAGE)
    .withCopyContentToContainer([{ content: config, target: "/mosquitto/config/mosquitto.conf" }])
    .withBindMounts([{ source: dataDir, target: "/mosquitto/data", mode: "rw" }])
    .withExposedPorts(1883)
    .withWaitStrategy(Wait.forLogMessage(/running/))
    .start();
  containers.push(container);
  const url = `mqtt://${container.getHost()}:${container.getMappedPort(1883)}`;
  const probe = connect(url, { protocolVersion: 5, clientId: `probe-${clientCounter++}`, reconnectPeriod: 250 });
  probe.on("error", () => undefined);
  await new Promise((resolve) => probe.once("connect", resolve));
  await probe.endAsync();
  return url;
}

/** Stops the newest broker cleanly (SIGTERM, so it saves) and leaves its data directory for the next one. */
async function stopCleanly(): Promise<void> {
  await containers.pop()?.stop({ timeout: 30_000 });
}

/** Kills the newest broker with SIGKILL: a crash or an OOM kill, with no chance to save or commit. */
function crash(): void {
  const container = containers[containers.length - 1];
  if (container) execFileSync("docker", ["kill", "-s", "KILL", container.getId()]);
}

/** Resolves after `ms` milliseconds. */
const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Resolves when `predicate` is true, polling; fails the test after `ms`. */
async function waitFor(predicate: () => boolean, ms = 15_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await pause(50);
  }
}

/** Publishes readings `from`..`to` for one device at QoS 1 and waits for the broker's PUBACKs. */
async function publish(url: string, from: number, to: number): Promise<void> {
  const client = connect(url, { protocolVersion: 5, clientId: `pub-${clientCounter++}`, reconnectPeriod: 250 });
  await new Promise((resolve) => client.once("connect", resolve));
  for (let seq = from; seq <= to; seq++) {
    const payload = JSON.stringify({ deviceId: "dev-1", epoch: 1, seq, timestamp: new Date().toISOString(), kind: "Inverter" });
    await client.publishAsync(telemetryTopic("gw-1", "dev-1"), payload, { qos: 1 });
  }
  await client.endAsync();
}

/** Connects a persistent shared-subscription member that acknowledges everything, as a running ingest replica does. */
async function member(url: string, clientId: string): Promise<{ client: MqttClient; seqs: number[]; sessionPresent: boolean }> {
  const seqs: number[] = [];
  const client = connect(url, { protocolVersion: 5, clientId, clean: false, properties: { sessionExpiryInterval: 3600 }, reconnectPeriod: 250, resubscribe: false });
  client.on("message", (_topic, payload) => seqs.push(JSON.parse(payload.toString()).seq));
  const connack = await new Promise<{ sessionPresent: boolean }>((resolve) => client.once("connect", (packet) => resolve(packet as { sessionPresent: boolean })));
  await client.subscribeAsync(SHARED, { qos: 1 });
  return { client, seqs, sessionPresent: connack.sessionPresent };
}

/** Returns the seq carried by a delivery's payload. */
const seqOf = (delivery: Delivery) => JSON.parse(Buffer.from(delivery.payload).toString()).seq as number;

describe("persist-sqlite canaries (2.1.2)", () => {
  it("#3758: an in-flight reading restored after a restart is redelivered without DUP, so only the redelivered label undercounts", async () => {
    let url = await startBroker("sqlite");
    const first: Delivery[] = [];
    const held = await startSubscriber({
      url, clientId: "ingest-1", receiveMaximum: 10, sessionExpirySeconds: 3600, keepaliveSeconds: 30, reconnectPeriodMs: 250,
      now: () => new Date(), onDelivery: (d) => first.push(d),
    });
    await publish(url, 1, 3);
    await waitFor(() => first.length === 3);
    await pause(FLUSH_PERIOD_MS + 2_000);
    await held.close();
    await stopCleanly();

    url = await startBroker("sqlite");
    const again: Delivery[] = [];
    const resumed = await startSubscriber({
      url, clientId: "ingest-1", receiveMaximum: 10, sessionExpirySeconds: 3600, keepaliveSeconds: 30, reconnectPeriodMs: 250,
      now: () => new Date(), onDelivery: (d) => { again.push(d); d.ack(); },
    });
    await waitFor(() => again.length === 3);
    await resumed.close();

    expect(again.map(seqOf).sort()).toEqual([1, 2, 3]);
    expect(first.every((d) => !d.redelivered)).toBe(true);
    expect(again.map((d) => d.redelivered), "#3758 fixed upstream? Update S6-14 and the redelivered-label note").toEqual([false, false, false]);
  });

  for (const backend of ["sqlite", "snapshot"] as const) {
    it(`live reconnect (${backend}): unacknowledged readings come back to the same session, and DUP says so`, async () => {
      const url = await startBroker(backend);
      const first: Delivery[] = [];
      const held = await startSubscriber({
        url, clientId: "ingest-1", receiveMaximum: 10, sessionExpirySeconds: 3600, keepaliveSeconds: 30, reconnectPeriodMs: 250,
        now: () => new Date(), onDelivery: (d) => first.push(d),
      });
      await publish(url, 1, 3);
      await waitFor(() => first.length === 3);
      await held.close();
      const again: Delivery[] = [];
      const resumed = await startSubscriber({
        url, clientId: "ingest-1", receiveMaximum: 10, sessionExpirySeconds: 3600, keepaliveSeconds: 30, reconnectPeriodMs: 250,
        now: () => new Date(), onDelivery: (d) => { again.push(d); d.ack(); },
      });
      await waitFor(() => again.length === 3);
      await resumed.close();

      expect(again.map(seqOf).sort()).toEqual([1, 2, 3]);
      expect(again.map((d) => d.redelivered), `DUP on a live reconnect (${backend}) changed; the redelivered label's meaning depends on it (S7-7)`).toEqual([true, true, true]);
    });
  }

  it("#3761: a member that ended its session one flush period before a crash stays gone", async () => {
    let url = await startBroker("sqlite");
    const stays = await member(url, "ingest-1");
    const leaves = await member(url, "ingest-2");
    await pause(FLUSH_PERIOD_MS + 2_000);
    await leaves.client.endAsync(false, { properties: { sessionExpiryInterval: 0 } });
    await pause(FLUSH_PERIOD_MS + 2_000);
    await stays.client.endAsync();
    crash();

    url = await startBroker("sqlite");
    const back = await member(url, "ingest-1");
    await publish(url, 1, 20);
    await waitFor(() => back.seqs.length === 20);
    await back.client.endAsync();

    expect(back.sessionPresent).toBe(true);
    expect(back.seqs.sort((a, b) => a - b)).toEqual(Array.from({ length: 20 }, (_, i) => i + 1));
  });

  it("#3761: a member that ended its session just before a crash comes back as a ghost and takes a share of readings", async () => {
    let url = await startBroker("sqlite");
    const stays = await member(url, "ingest-1");
    const leaves = await member(url, "ingest-2");
    await pause(FLUSH_PERIOD_MS + 2_000);
    await stays.client.endAsync();
    await leaves.client.endAsync(false, { properties: { sessionExpiryInterval: 0 } });
    crash();

    url = await startBroker("sqlite");
    const back = await member(url, "ingest-1");
    await publish(url, 1, 20);
    await pause(3_000);
    await back.client.endAsync();
    const ghost = await member(url, "ingest-2");
    await pause(1_500);
    await ghost.client.endAsync();

    expect(back.seqs.length, "#3761 fixed upstream? Update S6-14 and the scale-down runbook").toBeLessThan(20);
    expect(ghost.sessionPresent).toBe(true);
    expect(back.seqs.length + ghost.seqs.length).toBe(20);
  });

  it("no migration: switching a snapshot volume to persist-sqlite starts with no session and no queue", async () => {
    let url = await startBroker("snapshot");
    const offline = await member(url, "ingest-1");
    await offline.client.endAsync();
    await publish(url, 1, 50);
    await stopCleanly();

    url = await startBroker("sqlite");
    const resumed = await member(url, "ingest-1");
    await pause(2_000);
    await publish(url, 51, 55);
    await waitFor(() => resumed.seqs.length >= 5);
    await pause(500);
    await resumed.client.endAsync();

    expect(resumed.sessionPresent).toBe(false);
    expect(resumed.seqs.sort((a, b) => a - b)).toEqual([51, 52, 53, 54, 55]);
  });
});
