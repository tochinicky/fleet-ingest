/**
 * Checks D3's persistence premise: a persistent session's queued QoS 1 messages survive a broker restart under autosave,
 * and a crash loses only what came after the last snapshot.
 */
import type { Check } from "../check.ts";
import { crashAndStart, restartCleanly, sleep, startBroker, stopBroker } from "../broker.ts";
import { connectIngest, publishSequence } from "../clients.ts";

const TOPIC = "homes/gw-0001/devices/dev-01/telemetry";

/** Registers ingest's persistent session, then disconnects, so messages published afterwards queue for it. */
async function registerOfflineSession(url: string): Promise<void> {
  const ingest = await connectIngest({ url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 100, mode: "deferred", autoAck: true });
  await ingest.close();
}

/** Reconnects ingest with the same client ID, accepts everything that's queued, and returns the sequence numbers. */
async function collectQueued(url: string): Promise<number[]> {
  const ingest = await connectIngest({ url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 1000, mode: "deferred", autoAck: true });
  await sleep(3000);
  await ingest.close();
  return ingest.received.map((r) => r.seq);
}

/**
 * Proves: "queued QoS 1 messages survive a clean broker restart (SIGTERM) with persistence on and autosave_interval 60".
 * 500 messages queue for an offline session, the broker restarts, and ingest should then get all 500.
 */
export const queueSurvivesCleanRestart: Check = {
  id: "queue-survives-clean-restart",
  tier: "escalate",
  proves: "Queued QoS 1 messages for an offline persistent session survive a clean broker restart under autosave",
  run: async () => {
    const broker = await startBroker("persist-clean", ["autosave_interval 60"]);
    try {
      await registerOfflineSession(broker.url);
      await publishSequence(broker.url, TOPIC, 1, 500);
      await restartCleanly(broker);
      const seqs = await collectQueued(broker.url);
      return { pass: seqs.length === 500, observed: { queued: 500, deliveredAfterRestart: seqs.length } };
    } finally {
      stopBroker(broker);
    }
  },
};

/**
 * Measures the crash window: with `autosave_interval 5`, 500 messages are queued and the next snapshot is allowed to happen.
 * Then 300 more are queued and the broker is SIGKILLed straight away. The research expects the first 500 to survive and
 * the 300 to be lost. The spike uses 5 s instead of D3's 60 s; the mechanism is the same, the wait is shorter.
 */
export const crashLosesSinceLastSnapshot: Check = {
  id: "crash-loses-since-last-snapshot",
  tier: "escalate",
  proves: "After SIGKILL, queued messages up to the last autosave survive and only later ones are lost",
  run: async () => {
    const broker = await startBroker("persist-crash", ["autosave_interval 5"]);
    try {
      await registerOfflineSession(broker.url);
      await publishSequence(broker.url, TOPIC, 1, 500);
      await sleep(7000);
      await publishSequence(broker.url, TOPIC, 501, 800);
      await crashAndStart(broker);
      const seqs = await collectQueued(broker.url);
      const survivedFromBeforeSnapshot = seqs.filter((seq) => seq <= 500).length;
      const survivedFromAfterSnapshot = seqs.filter((seq) => seq > 500).length;
      return {
        pass: survivedFromBeforeSnapshot === 500,
        observed: { autosaveIntervalSeconds: 5, queuedBeforeSnapshot: 500, queuedAfterSnapshot: 300, survivedFromBeforeSnapshot, survivedFromAfterSnapshot },
      };
    } finally {
      stopBroker(broker);
    }
  },
};
