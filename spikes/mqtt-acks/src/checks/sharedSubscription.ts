/**
 * Live check of the shared-subscription failover research (D2): a killed member stays in the rotation, its share queues
 * and then drops, and its in-flight messages come back with DUP set when it returns with the same client ID.
 */
import type { Check } from "../check.ts";
import { sleep, startBroker, stopBroker } from "../broker.ts";
import { connectIngest, publishSequence, readSys } from "../clients.ts";

const PUBLISH_TOPIC = "homes/gw-0001/devices/dev-01/telemetry";
const SHARED_TOPIC = "$share/ingest/homes/+/devices/+/telemetry";
const DROPPED = "$SYS/broker/publish/messages/dropped";

/**
 * Proves or disproves three things the research read from the source:
 * - (a) the broker keeps sending to a dead member in rotation;
 * - (b) the dead member's share is dropped once its queue is full, not given to the live member;
 * - (c) the dead member's in-flight messages are redelivered with DUP when it resumes with the same client ID.
 *
 * Setup: `max_queued_messages 10`. ingest-1 holds its acks and is killed; ingest-2 acks everything.
 */
export const sharedSubscriptionFailover: Check = {
  id: "shared-subscription-failover",
  tier: "record",
  proves: "Dead shared-subscription member stays in rotation; its share queues then drops; its in-flight is redelivered with DUP on same-ID restart",
  run: async () => {
    const broker = await startBroker("shared-sub", ["max_queued_messages 10"]);
    try {
      const one = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: SHARED_TOPIC, receiveMaximum: 100, mode: "deferred" });
      const two = await connectIngest({ url: broker.url, clientId: "ingest-2", topic: SHARED_TOPIC, receiveMaximum: 100, mode: "deferred", autoAck: true });
      await publishSequence(broker.url, PUBLISH_TOPIC, 1, 4, 1);
      await sleep(500);
      const inflightAtKill = one.received.map((r) => r.seq);
      one.kill();
      await sleep(300);
      const droppedBefore = Number((await readSys(broker.url)).get(DROPPED) ?? NaN);
      await publishSequence(broker.url, PUBLISH_TOPIC, 5, 64, 1);
      await sleep(1500);
      const droppedAfter = Number((await readSys(broker.url)).get(DROPPED) ?? NaN);
      const liveMemberGotOf60 = two.received.filter((r) => r.seq >= 5).length;
      const back = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: SHARED_TOPIC, receiveMaximum: 100, mode: "deferred", autoAck: true });
      await sleep(2000);
      await back.close();
      await two.close();
      const unique = new Set([...two.received, ...back.received].map((r) => r.seq));
      return {
        pass: null,
        observed: {
          published: 64,
          inflightToIngest1AtKill: inflightAtKill,
          liveMemberReceivedOf60PublishedAfterKill: liveMemberGotOf60,
          droppedCounterDelta: droppedAfter - droppedBefore,
          ingest1OnResume: back.received.map((r) => `${r.seq}${r.dup ? "(DUP)" : ""}`),
          uniqueStored: unique.size,
          lost: 64 - unique.size,
        },
      };
    } finally {
      stopBroker(broker);
    }
  },
};
