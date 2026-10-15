/**
 * Measures the two numbers D3 sizes the broker from: resident memory with about 240k queued messages, and how long a
 * persistence snapshot of that queue takes (the broker is single-threaded, so a snapshot is a stall).
 */
import type { Check } from "../check.ts";
import { brokerPeakRssBytes, brokerRssBytes, logs, sleep, snapshotFileBytes, startBroker, stopBroker, triggerSnapshot } from "../broker.ts";
import { connectIngest, connectPublisher, publishSequence } from "../clients.ts";

const TOPIC = "homes/gw-0001/devices/dev-01/telemetry";
const QUEUED = 240_000;

/**
 * Measures how long the broker stalls: a probe client publishes to itself every 5 ms and records the longest gap
 * between round trips while `during` runs.
 */
async function longestStallMs(url: string, during: () => Promise<void>): Promise<number> {
  const probe = await connectPublisher(url, "probe");
  await probe.subscribeAsync("spike/probe", { qos: 0 });
  let lastEcho = Date.now();
  let longest = 0;
  probe.on("message", () => {
    const now = Date.now();
    longest = Math.max(longest, now - lastEcho);
    lastEcho = now;
  });
  const timer = setInterval(() => probe.publish("spike/probe", "x", { qos: 0 }), 5);
  await sleep(500);
  longest = 0;
  lastEcho = Date.now();
  await during();
  clearInterval(timer);
  await probe.endAsync();
  return longest;
}

/** Waits until the snapshot file's size has stopped changing for a second, and returns that size. */
async function waitForStableSnapshot(broker: Parameters<typeof snapshotFileBytes>[0]): Promise<number> {
  let previous = -1;
  let size = snapshotFileBytes(broker);
  while (size !== previous || size === 0) {
    previous = size;
    await sleep(1000);
    size = snapshotFileBytes(broker);
  }
  return size;
}

/**
 * Records the broker's RSS before and after queueing 240k ~300-byte messages for an offline session, the bytes per queued
 * message, the peak RSS while a SIGUSR1 snapshot of that queue is written (VmHWM, which sets the container limit),
 * and how long that snapshot stalls the broker.
 */
export const memoryAndSnapshotAt240k: Check = {
  id: "memory-and-snapshot-at-240k",
  tier: "record",
  proves: "Broker RSS at 240k queued messages, bytes per queued message, and the stall a full snapshot causes",
  run: async () => {
    const broker = await startBroker("memory-240k", ["max_queued_messages 250000", "autosave_interval 3600"]);
    try {
      const ingest = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 100, mode: "deferred", autoAck: true });
      await ingest.close();
      await sleep(1000);
      const rssBefore = brokerRssBytes(broker);
      const publishStart = Date.now();
      await publishSequence(broker.url, TOPIC, 1, QUEUED, 500);
      const publishSeconds = (Date.now() - publishStart) / 1000;
      await sleep(2000);
      const rssAfter = brokerRssBytes(broker);
      const logOffset = logs(broker.name).length;
      let snapshotBytes = 0;
      const stallMs = await longestStallMs(broker.url, async () => {
        triggerSnapshot(broker);
        snapshotBytes = await waitForStableSnapshot(broker);
      });
      const peakRssDuringSnapshot = brokerPeakRssBytes(broker);
      const snapshotLog = logs(broker.name).slice(logOffset).split("\n").filter((line) => /sav/i.test(line));
      return {
        pass: null,
        observed: {
          queued: QUEUED,
          publishRateMsgPerS: Math.round(QUEUED / publishSeconds),
          rssBeforeMB: +(rssBefore / 1e6).toFixed(1),
          rssAfterMB: +(rssAfter / 1e6).toFixed(1),
          peakRssIncludingSnapshotMB: +(peakRssDuringSnapshot / 1e6).toFixed(1),
          bytesPerQueuedMessage: Math.round((rssAfter - rssBefore) / QUEUED),
          snapshotFileMB: +(snapshotBytes / 1e6).toFixed(1),
          longestBrokerStallDuringSnapshotMs: stallMs,
          snapshotLog,
        },
      };
    } finally {
      stopBroker(broker);
    }
  },
};
