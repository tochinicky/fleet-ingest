/**
 * Slice 6b finding (scenario 3b): with both ingest replicas connected but stalled, the broker's drop counter came out
 * about 4% higher than the readings actually lost. This check isolates it: two connected shared-subscription members
 * that stop acknowledging, a small queue bound, and a numbered sequence. Afterwards, compare the counter's increase
 * with what never reached either member.
 */
import type { Check } from "../check.ts";
import { sleep, startBroker, stopBroker } from "../broker.ts";
import { connectIngest, publishSequence, readSys } from "../clients.ts";

const TOPIC = "homes/gw-0001/devices/dev-01/telemetry";
const SHARED = "$share/ingest/homes/+/devices/+/telemetry";
const DROPPED = "$SYS/broker/publish/messages/dropped";

/** Releases held acks on both members until nothing new arrives for a second. */
async function drainBoth(members: Array<Awaited<ReturnType<typeof connectIngest>>>): Promise<void> {
  let last = -1;
  const total = () => members.reduce((s, m) => s + m.received.length, 0);
  while (total() !== last) {
    last = total();
    members.forEach((m) => m.release());
    await sleep(1000);
  }
}

/** Records whether, with two stalled members, the drop counter equals the messages that reached neither. */
export const droppedCounterTwoStalledMembers: Check = {
  id: "dropped-counter-two-stalled-members",
  tier: "record",
  proves: "Whether $SYS dropped equals real loss when both shared-subscription members are connected but stalled",
  run: async () => {
    const broker = await startBroker("dropped-two", ["max_queued_messages 1000"]);
    try {
      const one = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: SHARED, receiveMaximum: 10, mode: "deferred" });
      const two = await connectIngest({ url: broker.url, clientId: "ingest-2", topic: SHARED, receiveMaximum: 10, mode: "deferred" });
      const before = Number((await readSys(broker.url)).get(DROPPED) ?? NaN);
      await publishSequence(broker.url, TOPIC, 1, 5000);
      await sleep(1500);
      const after = Number((await readSys(broker.url)).get(DROPPED) ?? NaN);
      await drainBoth([one, two]);
      const delivered = new Set([...one.received, ...two.received].map((r) => r.seq));
      one.kill();
      two.kill();
      return {
        pass: null,
        observed: {
          published: 5000, deliveredToEither: delivered.size, lost: 5000 - delivered.size,
          droppedCounterDelta: after - before, perMember: [one.received.length, two.received.length],
          duplicatesAcrossMembers: one.received.length + two.received.length - delivered.size,
        },
      };
    } finally {
      stopBroker(broker);
    }
  },
};

/**
 * The same with retained QoS 1 messages on per-gateway status topics, like the gateways' statuses: are they dropped
 * and counted like readings, when both members are stalled?
 */
export const droppedCounterRetainedStatuses: Check = {
  id: "dropped-counter-retained-statuses",
  tier: "record",
  proves: "Whether retained status messages are dropped and counted like readings when both shared members are stalled",
  run: async () => {
    const broker = await startBroker("dropped-retained", ["max_queued_messages 1000"]);
    try {
      const statusTopic = "$share/ingest/homes/+/status";
      const one = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: statusTopic, receiveMaximum: 10, mode: "deferred" });
      const two = await connectIngest({ url: broker.url, clientId: "ingest-2", topic: statusTopic, receiveMaximum: 10, mode: "deferred" });
      const before = Number((await readSys(broker.url)).get(DROPPED) ?? NaN);
      const { connectPublisher } = await import("../clients.ts");
      const publisher = await connectPublisher(broker.url);
      for (let i = 1; i <= 5000; i++) {
        await publisher.publishAsync(`homes/gw-${String(i % 500).padStart(4, "0")}/status`, JSON.stringify({ state: "online", seq: i }), { qos: 1, retain: true });
      }
      await publisher.endAsync();
      await sleep(1500);
      const after = Number((await readSys(broker.url)).get(DROPPED) ?? NaN);
      await drainBoth([one, two]);
      const delivered = one.received.length + two.received.length;
      one.kill();
      two.kill();
      return { pass: null, observed: { published: 5000, delivered, lost: 5000 - delivered, droppedCounterDelta: after - before } };
    } finally {
      stopBroker(broker);
    }
  },
};
