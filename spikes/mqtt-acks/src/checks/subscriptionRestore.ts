/**
 * Found in slice 3 (scenario 3b → 7): after a clean broker restart, ingest's restored persistent sessions received
 * nothing. This check pins down which subscriptions Mosquitto 2.1.2 restores from its persistence snapshot, and
 * what the client is told (CONNACK "session present").
 */
import type { Check } from "../check.ts";
import { restartCleanly, sleep, startBroker, stopBroker } from "../broker.ts";
import { connectIngest, publishSequence } from "../clients.ts";

const TOPIC = "homes/gw-0001/devices/dev-01/telemetry";

/**
 * Records, for a plain and a shared subscription: is the session reported as present after a clean restart, and does
 * a message published after the restart reach the resumed session without subscribing again?
 */
/**
 * Records whether messages already queued for an offline shared-subscription session survive a clean restart, and are
 * delivered once the session resumes and subscribes again. This is the D3 guarantee across a broker restart.
 */
export const sharedBacklogSurvivesRestart: Check = {
  id: "shared-backlog-survives-restart",
  tier: "escalate",
  proves: "A backlog queued in an offline shared-subscription session survives a clean broker restart and is delivered after re-subscribing",
  run: async () => {
    const broker = await startBroker("shared-backlog", ["autosave_interval 60"]);
    try {
      const shared = await connectIngest({ url: broker.url, clientId: "shared-1", topic: "$share/ingest/homes/+/devices/+/telemetry", receiveMaximum: 100, mode: "deferred", autoAck: true });
      await shared.close();
      await publishSequence(broker.url, TOPIC, 1, 500);
      await restartCleanly(broker);
      const back = await connectIngest({ url: broker.url, clientId: "shared-1", topic: "$share/ingest/homes/+/devices/+/telemetry", receiveMaximum: 1000, mode: "deferred", autoAck: true });
      await sleep(3000);
      await back.close();
      const unique = new Set(back.received.map((r) => r.seq));
      return { pass: unique.size === 500, observed: { queuedBeforeRestart: 500, deliveredAfterResubscribe: unique.size } };
    } finally {
      stopBroker(broker);
    }
  },
};

export const subscriptionsSurviveRestart: Check = {
  id: "subscriptions-survive-restart",
  tier: "record",
  proves: "Whether plain and shared subscriptions of a persistent session are restored after a clean broker restart",
  run: async () => {
    const broker = await startBroker("sub-restore", ["autosave_interval 60"]);
    try {
      const plain = await connectIngest({ url: broker.url, clientId: "plain-1", topic: "homes/+/devices/+/telemetry", receiveMaximum: 100, mode: "deferred", autoAck: true });
      const shared = await connectIngest({ url: broker.url, clientId: "shared-1", topic: "$share/ingest/homes/+/devices/+/telemetry", receiveMaximum: 100, mode: "deferred", autoAck: true });
      await plain.close();
      await shared.close();
      await restartCleanly(broker);
      const resumed = async (clientId: string) => {
        const { connect } = await import("mqtt");
        const client = connect(broker.url, { clientId, protocolVersion: 5, clean: false, properties: { sessionExpiryInterval: 3600 }, reconnectPeriod: 0, resubscribe: false });
        const received: string[] = [];
        client.on("message", (_t, p) => received.push(p.toString()));
        const connack = await new Promise<{ sessionPresent: boolean }>((resolve) => client.once("connect", (packet) => resolve(packet as { sessionPresent: boolean })));
        return { client, received, sessionPresent: connack.sessionPresent };
      };
      const p = await resumed("plain-1");
      const s = await resumed("shared-1");
      await publishSequence(broker.url, TOPIC, 1, 5);
      await sleep(1500);
      await p.client.endAsync();
      await s.client.endAsync();
      return {
        pass: null,
        observed: {
          plain: { sessionPresent: p.sessionPresent, receivedAfterRestart: p.received.length },
          shared: { sessionPresent: s.sessionPresent, receivedAfterRestart: s.received.length },
        },
      };
    } finally {
      stopBroker(broker);
    }
  },
};
