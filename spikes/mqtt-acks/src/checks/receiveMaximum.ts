/**
 * Checks how many unacknowledged messages the broker sends ingest at once. This caps the in-memory batch (D3, D5).
 */
import type { Check } from "../check.ts";
import { sleep, startBroker, stopBroker } from "../broker.ts";
import { connectIngest, publishSequence } from "../clients.ts";

const TOPIC = "homes/gw-0001/devices/dev-01/telemetry";

/**
 * Proves: "the broker never has more than ingest's Receive Maximum unacknowledged deliveries outstanding". With RM 10 and
 * nothing acked, exactly 10 should arrive. After releasing 5 acks, exactly 5 more should follow.
 */
export const brokerHonoursReceiveMaximum: Check = {
  id: "broker-honours-receive-maximum",
  tier: "stop",
  proves: "The broker never exceeds ingest's Receive Maximum of unacked deliveries",
  run: async () => {
    const broker = await startBroker("receive-maximum");
    try {
      const ingest = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 10, mode: "deferred" });
      await publishSequence(broker.url, TOPIC, 1, 50);
      await sleep(1500);
      const beforeRelease = ingest.received.length;
      ingest.release(5);
      await sleep(1500);
      const afterReleasingFive = ingest.received.length;
      ingest.release();
      ingest.kill();
      return {
        pass: beforeRelease === 10 && afterReleasingFive === 15,
        observed: { receiveMaximum: 10, published: 50, beforeRelease, afterReleasingFive },
      };
    } finally {
      stopBroker(broker);
    }
  },
};

/**
 * Measures how much the broker actually has in flight to ingest for a given Receive Maximum and `max_inflight_messages`,
 * to test "in-flight = min(RM, broker setting)". D3 asks for RM 1,000, and Mosquitto's default `max_inflight_messages` is 20.
 */
async function inflightFor(name: string, receiveMaximum: number, brokerConfig: string[]): Promise<number> {
  const broker = await startBroker(name, brokerConfig);
  try {
    const ingest = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: TOPIC, receiveMaximum, mode: "deferred" });
    await publishSequence(broker.url, TOPIC, 1, 2000, 200);
    await sleep(2000);
    const inflight = ingest.received.length;
    ingest.release();
    ingest.kill();
    return inflight;
  } finally {
    stopBroker(broker);
  }
}

/**
 * Records the effective in-flight window to ingest for three settings: RM 1,000 with the broker default, RM 1,000 with
 * `max_inflight_messages 1000`, and RM 10 with `max_inflight_messages 1000`.
 */
export const inflightIsMinOfReceiveMaximumAndBroker: Check = {
  id: "inflight-is-min-of-rm-and-broker",
  tier: "record",
  proves: "In-flight to ingest = min(ingest Receive Maximum, broker max_inflight_messages) on MQTT v5",
  run: async () => {
    const rm1000BrokerDefault = await inflightFor("inflight-default", 1000, []);
    const rm1000Broker1000 = await inflightFor("inflight-1000", 1000, ["max_inflight_messages 1000"]);
    const rm10Broker1000 = await inflightFor("inflight-rm10", 10, ["max_inflight_messages 1000"]);
    return { pass: null, observed: { rm1000BrokerDefault, rm1000Broker1000, rm10Broker1000 } };
  },
};
