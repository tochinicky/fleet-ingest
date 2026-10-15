/**
 * Checks whether ingest can hold PUBACKs until a commit and still receive a whole batch in the meantime.
 * Found while reading mqtt.js 5.16.0 (`build/lib/client.js:350`, `handlers/publish.js:97-117`): the client reads
 * the next incoming packet only after the current PUBACK has been written. If that's right, holding the documented
 * `customHandleAcks` callback lets just one message in at a time.
 */
import type { Check } from "../check.ts";
import { sleep, startBroker, stopBroker } from "../broker.ts";
import { connectIngest, publishSequence, waitUntil } from "../clients.ts";

const TOPIC = "homes/gw-0001/devices/dev-01/telemetry";

/**
 * Proves or disproves: "holding the documented `customHandleAcks` callback stops mqtt.js from receiving any further
 * messages, so a batch can't fill up". It publishes 20 messages, holds every ack, and counts how many arrive.
 */
export const documentedHoldBlocksReceiving: Check = {
  id: "documented-hold-blocks-receiving",
  tier: "record",
  proves: "Holding the documented customHandleAcks callback lets only one message in at a time (head-of-line blocking)",
  run: async () => {
    const broker = await startBroker("documented-hold");
    try {
      const ingest = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 100, mode: "documented" });
      await publishSequence(broker.url, TOPIC, 1, 20);
      await sleep(1500);
      const receivedWhileHolding = ingest.received.length;
      const drainStart = Date.now();
      while (ingest.received.length < 20 && Date.now() - drainStart < 10_000) {
        ingest.release();
        await sleep(5);
      }
      ingest.release();
      await ingest.close();
      return {
        pass: null,
        observed: { published: 20, receivedWhileHolding, receivedAfterReleasingOneByOne: ingest.received.length },
      };
    } finally {
      stopBroker(broker);
    }
  },
};

/**
 * Proves or disproves: "while a documented ack is held, keepalive still works". A held callback also blocks
 * PINGRESP, so a commit slower than the keepalive could make mqtt.js drop a healthy connection.
 */
export const documentedHoldStallsKeepalive: Check = {
  id: "documented-hold-stalls-keepalive",
  tier: "record",
  proves: "A held customHandleAcks callback also blocks PINGRESP, so a commit slower than the keepalive drops the connection",
  run: async () => {
    const broker = await startBroker("documented-keepalive");
    try {
      const ingest = await connectIngest({
        url: broker.url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 100, mode: "documented", keepaliveSeconds: 2,
      });
      const events: string[] = [];
      ingest.client.on("close", () => events.push("close"));
      ingest.client.on("error", (error) => events.push(`error: ${error.message}`));
      await publishSequence(broker.url, TOPIC, 1, 1);
      await waitUntil(() => ingest.received.length === 1, 5000, "first message");
      await sleep(8000);
      const connectedAfterHold = ingest.client.connected;
      ingest.release();
      ingest.kill();
      return { pass: null, observed: { keepaliveSeconds: 2, heldForSeconds: 8, connectedAfterHold, events } };
    } finally {
      stopBroker(broker);
    }
  },
};

/**
 * Proves: "with PUBACKs held at the packet writer, keepalive keeps working". It holds one ack for more than 3× the
 * keepalive, and checks that the connection stays up and that more messages still arrive during the hold.
 */
export const deferredHoldKeepsKeepalive: Check = {
  id: "deferred-hold-keeps-keepalive",
  tier: "stop",
  proves: "With PUBACKs held at the packet writer for over 3x keepalive, PINGRESP still flows and the connection stays up",
  run: async () => {
    const broker = await startBroker("deferred-keepalive");
    try {
      const ingest = await connectIngest({
        url: broker.url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 100, mode: "deferred", keepaliveSeconds: 2,
      });
      const events: string[] = [];
      let pingresps = 0;
      ingest.client.on("packetreceive", (packet) => {
        if (packet.cmd === "pingresp") pingresps += 1;
      });
      ingest.client.on("close", () => events.push("close"));
      ingest.client.on("error", (error) => events.push(`error: ${error.message}`));
      await publishSequence(broker.url, TOPIC, 1, 1);
      await waitUntil(() => ingest.received.length === 1, 5000, "first message");
      await sleep(7000);
      await publishSequence(broker.url, TOPIC, 2, 2);
      await sleep(500);
      const connectedAfterHold = ingest.client.connected;
      const receivedDuringHold = ingest.received.length;
      const stillHeld = ingest.pending();
      const eventsDuringHold = [...events];
      ingest.release();
      await ingest.close();
      return {
        pass: connectedAfterHold && pingresps >= 2 && receivedDuringHold === 2 && stillHeld === 2 && eventsDuringHold.length === 0,
        observed: { keepaliveSeconds: 2, heldForSeconds: 7.5, connectedAfterHold, pingresps, receivedDuringHold, stillHeld, eventsDuringHold },
      };
    } finally {
      stopBroker(broker);
    }
  },
};

/**
 * Proves: "ingest can receive a whole batch while every PUBACK is held, then send the PUBACKs in arrival order after
 * the commit, and the broker takes them as acknowledgements". The last step reconnects with the same client ID and
 * checks that nothing is redelivered.
 */
export const deferredPubackAllowsBatching: Check = {
  id: "deferred-puback-allows-batching",
  tier: "stop",
  proves: "With PUBACKs held at the packet writer, a full batch arrives unacked, and acks released after commit (in arrival order) are honoured by the broker",
  run: async () => {
    const broker = await startBroker("deferred-batching");
    try {
      const first = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 100, mode: "deferred" });
      await publishSequence(broker.url, TOPIC, 1, 20);
      await waitUntil(() => first.received.length === 20, 5000, "20 messages");
      const receivedWhileHolding = first.received.length;
      const heldAcks = first.pending();
      await sleep(300);
      first.release();
      await sleep(300);
      await first.close();
      const second = await connectIngest({ url: broker.url, clientId: "ingest-1", topic: TOPIC, receiveMaximum: 100, mode: "deferred", autoAck: true });
      await sleep(1500);
      const redelivered = second.received.length;
      await second.close();
      const arrivalOrder = first.received.map((r) => r.seq);
      const inOrder = arrivalOrder.every((seq, i) => seq === i + 1);
      return {
        pass: receivedWhileHolding === 20 && heldAcks === 20 && redelivered === 0 && inOrder,
        observed: { receivedWhileHolding, heldAcks, arrivalOrderIs1to20: inOrder, redeliveredAfterReleaseAndResume: redelivered },
      };
    } finally {
      stopBroker(broker);
    }
  },
};
