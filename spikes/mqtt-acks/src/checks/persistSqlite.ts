/**
 * Slice 6 experiment, measurement only (D3; switching is a separate decision, S7-7): the persist-sqlite plugin against the
 * snapshot persistence the stack uses. Three questions:
 * 1. Does it restore a persistent session's *shared* subscription after a clean restart? Snapshots don't (S3-9).
 * 2. How much acknowledged queue does a SIGKILL lose? Snapshots lose everything since the last autosave.
 * 3. What does it cost: broker RSS with 240k queued, and the longest broker stall while it persists them?
 *    A full snapshot of 240k stalls the broker about 3.2 s (S0-6).
 */
import type { Check } from "../check.ts";
import { brokerRssBytes, crashAndStart, restartCleanly, sleep, startBroker, stopBroker } from "../broker.ts";
import { connectIngest, connectPublisher, publishSequence } from "../clients.ts";

const TOPIC = "homes/gw-0001/devices/dev-01/telemetry";
const SHARED = "$share/ingest/homes/+/devices/+/telemetry";

/** Registers an offline persistent session on `topic`, then disconnects, so later publishes queue for it. */
async function registerOffline(url: string, clientId: string, topic: string): Promise<void> {
  const ingest = await connectIngest({ url, clientId, topic, receiveMaximum: 100, mode: "deferred", autoAck: true });
  await ingest.close();
}

/** Resumes the session WITHOUT subscribing again (resubscribe off), and returns the seqs that arrive within 3 s. */
async function resumeWithoutSubscribing(url: string, clientId: string): Promise<{ sessionPresent: boolean; seqs: number[] }> {
  const { connect } = await import("mqtt");
  const client = connect(url, { clientId, protocolVersion: 5, clean: false, properties: { sessionExpiryInterval: 3600, receiveMaximum: 1000 }, reconnectPeriod: 0, resubscribe: false });
  const seqs: number[] = [];
  client.on("message", (_t, payload) => seqs.push(JSON.parse(payload.toString()).seq));
  const connack = await new Promise<{ sessionPresent: boolean }>((resolve) => client.once("connect", (packet) => resolve(packet as { sessionPresent: boolean })));
  await sleep(3000);
  await client.endAsync();
  return { sessionPresent: connack.sessionPresent, seqs };
}

/** Q1: after a clean restart, does a resumed shared-subscription session get new messages without subscribing again? */
export const sqliteRestoresSharedSubscriptions: Check = {
  id: "sqlite-restores-shared-subscriptions",
  tier: "record",
  proves: "Whether persist-sqlite restores a shared subscription after a clean restart (snapshots don't, S3-9)",
  run: async () => {
    const broker = await startBroker("sqlite-shared", [], "sqlite");
    try {
      await registerOffline(broker.url, "shared-1", SHARED);
      await publishSequence(broker.url, TOPIC, 1, 50);
      await restartCleanly(broker);
      const resumeAndPublish = resumeWithoutSubscribing(broker.url, "shared-1");
      await sleep(500);
      await publishSequence(broker.url, TOPIC, 51, 60);
      const resumed = await resumeAndPublish;
      return {
        pass: null,
        observed: {
          sessionPresent: resumed.sessionPresent,
          queuedBeforeRestartDelivered: resumed.seqs.filter((s) => s <= 50).length,
          publishedAfterRestartDelivered: resumed.seqs.filter((s) => s > 50).length,
        },
      };
    } finally {
      stopBroker(broker);
    }
  },
};

/** Q2: queue 500, wait past one flush, queue 300 more, SIGKILL straight away: how many of each survive? */
export const sqliteCrashWindow: Check = {
  id: "sqlite-crash-window",
  tier: "record",
  proves: "How much acknowledged queue persist-sqlite loses on SIGKILL (flush_period 5 s)",
  run: async () => {
    const broker = await startBroker("sqlite-crash", [], "sqlite");
    try {
      await registerOffline(broker.url, "ingest-1", TOPIC);
      await publishSequence(broker.url, TOPIC, 1, 500);
      await sleep(7000);
      await publishSequence(broker.url, TOPIC, 501, 800);
      await crashAndStart(broker);
      const ingest = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 1000, mode: "deferred", autoAck: true });
      await sleep(3000);
      await ingest.close();
      const seqs = ingest.received.map((r) => r.seq);
      return {
        pass: null,
        observed: {
          flushPeriodSeconds: 5,
          survivedFromBeforeFlush: seqs.filter((s) => s <= 500).length, ofBeforeFlush: 500,
          survivedFromLast300: seqs.filter((s) => s > 500).length, ofLast: 300,
        },
      };
    } finally {
      stopBroker(broker);
    }
  },
};

/**
 * Q3: queue 240k for an offline session while a probe measures the broker's longest stall, then report RSS. The
 * probe publishes to itself every 5 ms; the longest gap between echoes is the longest stall.
 */
export const sqliteCostAt240k: Check = {
  id: "sqlite-cost-at-240k",
  tier: "record",
  proves: "persist-sqlite's cost with 240k queued: broker RSS, publish rate, and the longest stall while it persists",
  run: async () => {
    const broker = await startBroker("sqlite-240k", ["max_queued_messages 250000"], "sqlite");
    try {
      await registerOffline(broker.url, "ingest-1", TOPIC);
      const probe = await connectPublisher(broker.url, "probe");
      await probe.subscribeAsync("spike/probe", { qos: 0 });
      let last = Date.now();
      let longest = 0;
      probe.on("message", () => {
        const now = Date.now();
        longest = Math.max(longest, now - last);
        last = now;
      });
      const timer = setInterval(() => probe.publish("spike/probe", "x", { qos: 0 }), 5);
      await sleep(500);
      longest = 0;
      last = Date.now();
      const started = Date.now();
      await publishSequence(broker.url, TOPIC, 1, 240_000, 500);
      const publishSeconds = (Date.now() - started) / 1000;
      await sleep(12_000);
      clearInterval(timer);
      await probe.endAsync();
      return {
        pass: null,
        observed: {
          queued: 240_000, publishRateMsgPerS: Math.round(240_000 / publishSeconds),
          rssMB: +(brokerRssBytes(broker) / 1e6).toFixed(1), longestBrokerStallMs: longest,
        },
      };
    } finally {
      stopBroker(broker);
    }
  },
};
