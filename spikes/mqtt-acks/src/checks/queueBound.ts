/**
 * Measures the broker queue bound (D3): how many messages a session can hold, when the overflow is dropped, whether the
 * drop counter sees it, and which `$SYS` topics exist on 2.1.2 for drops and stored messages.
 */
import type { Check } from "../check.ts";
import { logs, sleep, startBroker, stopBroker } from "../broker.ts";
import { connectIngest, publishSequence, readSys } from "../clients.ts";

const TOPIC = "homes/gw-0001/devices/dev-01/telemetry";
const DROPPED = "$SYS/broker/publish/messages/dropped";

/** Releases held acks until no new messages have arrived for a whole second, so the session queue is fully drained. */
async function drain(ingest: Awaited<ReturnType<typeof connectIngest>>): Promise<void> {
  let last = -1;
  while (ingest.received.length !== last) {
    last = ingest.received.length;
    ingest.release();
    await sleep(1000);
  }
}

/**
 * Records the bound for an offline persistent session with `max_queued_messages 1000`: 2,000 are published while ingest
 * is away, and the check counts how many it gets back and how far the drop counter moves.
 */
export const offlineQueueBound: Check = {
  id: "offline-queue-bound",
  tier: "record",
  proves: "An offline session keeps max_queued_messages and drops the rest, and the drop counter counts them",
  run: async () => {
    const broker = await startBroker("queue-offline", ["max_queued_messages 1000"]);
    try {
      const first = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 100, mode: "deferred", autoAck: true });
      await first.close();
      const droppedBefore = Number((await readSys(broker.url)).get(DROPPED) ?? NaN);
      await publishSequence(broker.url, TOPIC, 1, 2000);
      await sleep(1500);
      const droppedAfter = Number((await readSys(broker.url)).get(DROPPED) ?? NaN);
      const second = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 100, mode: "deferred", autoAck: true });
      await sleep(2000);
      await second.close();
      const seqs = second.received.map((r) => r.seq);
      return {
        pass: null,
        observed: {
          maxQueuedMessages: 1000, published: 2000, delivered: seqs.length,
          firstSeq: seqs[0], lastSeq: seqs.at(-1), droppedCounterDelta: droppedAfter - droppedBefore,
          dropLogLines: logs(broker.name).split("\n").filter((line) => /drop/i.test(line)).slice(0, 3),
        },
      };
    } finally {
      stopBroker(broker);
    }
  },
};

/**
 * Records the bound for a connected session that has stopped acknowledging (a slow database): RM 10, `max_queued_messages
 * 1000`, 2,000 published. The research expects max_queued + RM to survive.
 */
export const connectedStalledQueueBound: Check = {
  id: "connected-stalled-queue-bound",
  tier: "record",
  proves: "A connected but stalled session holds max_queued_messages + Receive Maximum before dropping",
  run: async () => {
    const broker = await startBroker("queue-stalled", ["max_queued_messages 1000"]);
    try {
      const ingest = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 10, mode: "deferred" });
      const droppedBefore = Number((await readSys(broker.url)).get(DROPPED) ?? NaN);
      await publishSequence(broker.url, TOPIC, 1, 2000);
      await sleep(1500);
      const droppedAfter = Number((await readSys(broker.url)).get(DROPPED) ?? NaN);
      await drain(ingest);
      ingest.kill();
      return {
        pass: null,
        observed: {
          receiveMaximum: 10, maxQueuedMessages: 1000, published: 2000,
          deliveredInTotal: ingest.received.length, droppedCounterDelta: droppedAfter - droppedBefore,
        },
      };
    } finally {
      stopBroker(broker);
    }
  },
};

/** Records every `$SYS` topic about drops, stored messages, queues, in-flight counts or heap that 2.1.2 actually publishes. */
export const sysTopicsAvailable: Check = {
  id: "sys-topics-available",
  tier: "record",
  proves: "Which $SYS topics exist on 2.1.2 for drops, stored/queued messages and memory",
  run: async () => {
    const broker = await startBroker("sys-topics");
    try {
      await publishSequence(broker.url, TOPIC, 1, 5);
      const values = await readSys(broker.url, 3000);
      const relevant = [...values.entries()]
        .filter(([topic]) => /drop|store|queue|inflight|heap|messages/i.test(topic))
        .sort(([a], [b]) => a.localeCompare(b));
      return { pass: null, observed: { totalSysTopics: values.size, relevant: Object.fromEntries(relevant) } };
    } finally {
      stopBroker(broker);
    }
  },
};
