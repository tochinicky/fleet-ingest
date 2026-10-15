/**
 * Checks that the broker keeps a persistent session's unacknowledged messages and redelivers them when the same client ID
 * comes back. This is what makes "crash before commit" safe in D3.
 */
import type { Check } from "../check.ts";
import { sleep, startBroker, stopBroker } from "../broker.ts";
import { connectIngest, publishSequence, waitUntil } from "../clients.ts";

const TOPIC = "homes/gw-0001/devices/dev-01/telemetry";

/**
 * Proves: "an unacked QoS 1 message is redelivered, with DUP set, after the session resumes; acked messages are not;
 * messages published while ingest was down are delivered too". Ingest takes 10 messages, acks 5, and loses its socket
 * without a DISCONNECT (like `kill -9`). Ten more are published, then the same client ID reconnects.
 */
export const unackedRedeliveredAfterResume: Check = {
  id: "unacked-redelivered-after-resume",
  tier: "stop",
  proves: "An unacked QoS 1 message is redelivered (DUP=1) after reconnecting with the same client ID; acked ones are not",
  run: async () => {
    const broker = await startBroker("redelivery");
    try {
      const first = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 100, mode: "deferred" });
      await publishSequence(broker.url, TOPIC, 1, 10);
      await waitUntil(() => first.received.length === 10, 5000, "10 messages");
      first.release(5);
      await sleep(300);
      first.kill();
      await publishSequence(broker.url, TOPIC, 11, 20);
      const second = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 100, mode: "deferred", autoAck: true });
      await waitUntil(() => second.received.length >= 15, 5000, "15 messages after resume").catch(() => undefined);
      await sleep(500);
      await second.close();
      const seqs = second.received.map((r) => r.seq);
      const dupSeqs = second.received.filter((r) => r.dup).map((r) => r.seq);
      const expected = Array.from({ length: 15 }, (_, i) => i + 6);
      const exact = seqs.length === 15 && expected.every((seq, i) => seqs[i] === seq);
      const dupOnlyOnUnacked = dupSeqs.join() === "6,7,8,9,10";
      return {
        pass: exact && dupOnlyOnUnacked,
        observed: { receivedAfterResume: seqs, dupFlagged: dupSeqs },
      };
    } finally {
      stopBroker(broker);
    }
  },
};
