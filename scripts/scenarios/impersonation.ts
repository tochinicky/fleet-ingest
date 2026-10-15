/**
 * Scenario 6: impersonation and revocation, against the running mTLS stack.
 * 1. With its own valid certificate, a gateway publishes on another gateway's topic. The broker refuses it (ACL by
 *    certificate CN), and nothing is stored.
 * 2. A gateway publishes, under its own topic, a reading for another gateway's device. The broker allows it, because
 *    the topic is its own, but ingest's device binding dead-letters it as `foreign_device` (defence in depth).
 * 3. A newly enrolled gateway connects and its reading (seq 1) is stored. Its certificate is then revoked and the
 *    broker sent SIGHUP to reload the CRL, and a new handshake is refused. Then two separate things are judged
 *    (S7-11, following the revocation runbook):
 *    - **the open connection** (a KNOWN LIMIT, S4-3, never a pass): the CRL is checked only at the handshake, so seq 2,
 *      sent on the connection that was open before the reload, is expected to be stored, and is reported as such;
 *    - **after the runbook's broker restart** (the security check): the old connection is gone, its reconnect and a
 *      fresh handshake are refused as revoked, and nothing from the gateway is stored after the restart (seqs 3, 4).
 *      Any row received after the restart is a FAIL.
 *
 * Usage: `node scripts/scenarios/impersonation.ts`. It brings the stack up itself if it isn't (S7-11: run first after
 * a `compose down`, it used to fail on its own setup).
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { connect } from "mqtt";
import type { MqttClient } from "mqtt";
import { compose, ensureFleet, finish, openDatabase, progress, run, sleep } from "./lib.ts";

const ROOT = join(import.meta.dirname, "..", "..");
const CERTS = join(ROOT, "deploy", "certs");

/** Connects as the certificate `cn`; resolves with the client, or with the refusal message. */
async function connectAs(cn: string, clientId = cn): Promise<MqttClient | string> {
  const client = connect("mqtts://127.0.0.1:8883", {
    protocolVersion: 5, clientId, reconnectPeriod: 0, connectTimeout: 5000, rejectUnauthorized: true,
    ca: readFileSync(join(CERTS, "ca.crt")), cert: readFileSync(join(CERTS, `${cn}.crt`)), key: readFileSync(join(CERTS, `${cn}.key`)),
  });
  return new Promise((resolve) => {
    client.once("connect", () => resolve(client));
    client.once("error", (error) => {
      client.end(true);
      resolve(error.message);
    });
    client.once("close", () => resolve("connection closed"));
  });
}

/** A telemetry payload with a recognisable seq, so the checks can find it in Postgres. */
function reading(deviceId: string, seq: number): string {
  return JSON.stringify({ deviceId, epoch: 1, seq, timestamp: new Date().toISOString(), kind: "Battery", socPercent: 50 });
}

/** Publishes at QoS 1 and returns "accepted" or the broker's refusal. */
async function publish(client: MqttClient, topic: string, payload: string): Promise<string> {
  return client.publishAsync(topic, payload, { qos: 1 }).then(() => "accepted", (error: Error) => error.message);
}

/** Runs the scenario and prints its verdicts. */
async function main(): Promise<void> {
  const started = Date.now();
  const pool = openDatabase();
  await ensureFleet(pool, Number(process.env.GATEWAY_COUNT ?? 500));
  const marker = 900_000_000 + Math.floor(Math.random() * 1_000_000);
  const enrolled = `scenario-gw-${marker}`;
  execFileSync(join(ROOT, "scripts", "pki.sh"), ["issue", CERTS, enrolled], { stdio: "ignore" });
  compose(["kill", "-s", "HUP", "mosquitto"]);
  await sleep(1000);

  progress(started, "1: sim-gw-0001 publishes on sim-gw-0002's topic");
  const gw1 = await connectAs("sim-gw-0001", "scenario-gw1");
  if (typeof gw1 === "string") throw new Error(`sim-gw-0001 refused: ${gw1}`);
  const crossTopic = await publish(gw1, "homes/sim-gw-0002/devices/sim-gw-0002-bat-3/telemetry", reading("sim-gw-0002-bat-3", marker));

  progress(started, "2: sim-gw-0001 publishes sim-gw-0002's device under its own topic");
  const ownTopicForeignDevice = await publish(gw1, "homes/sim-gw-0001/devices/sim-gw-0002-bat-3/telemetry", reading("sim-gw-0002-bat-3", marker + 1));
  await gw1.endAsync();

  progress(started, `3: ${enrolled} enrols, publishes, is revoked`);
  const fresh = await connectAs(enrolled);
  if (typeof fresh === "string") throw new Error(`${enrolled} refused before revocation: ${fresh}`);
  const beforeRevocation = await publish(fresh, `homes/${enrolled}/devices/${enrolled}-bat-1/telemetry`, reading(`${enrolled}-bat-1`, 1));
  execFileSync(join(ROOT, "scripts", "pki.sh"), ["revoke", CERTS, enrolled], { stdio: "ignore" });
  compose(["kill", "-s", "HUP", "mosquitto"]);
  await sleep(1500);
  const reconnect = await connectAs(enrolled, `${enrolled}-again`);
  const refusedAfterRevocation = typeof reconnect === "string";
  if (typeof reconnect !== "string") await reconnect.endAsync();
  const openConnectionAfterRevocation = await publish(fresh, `homes/${enrolled}/devices/${enrolled}-bat-1/telemetry`, reading(`${enrolled}-bat-1`, 2));

  progress(started, "3b: the runbook's next step: restart the broker to drop the open connection");
  const restartedAt = new Date();
  compose(["restart", "mosquitto"]);
  // The broker is back for clients once a valid gateway can connect again; only then does a refusal mean "revoked".
  const readyBy = Date.now() + 60_000;
  for (let probe = await connectAs("sim-gw-0001", "scenario-ready"); ; probe = await connectAs("sim-gw-0001", "scenario-ready")) {
    if (typeof probe !== "string") { await probe.endAsync(); break; }
    if (Date.now() > readyBy) throw new Error(`the broker didn't accept a valid gateway within 60 s of its restart: ${probe}`);
    await sleep(1000);
  }
  const oldConnectionAfterRestart = fresh.connected
    ? await publish(fresh, `homes/${enrolled}/devices/${enrolled}-bat-1/telemetry`, reading(`${enrolled}-bat-1`, 3))
    : "not connected";
  await fresh.endAsync(true).catch(() => undefined);
  const afterRestart = await connectAs(enrolled, `${enrolled}-after-restart`);
  const refusedAfterRestart = typeof afterRestart === "string" && /revoked/i.test(afterRestart);
  let publishedAfterRestart = "no connection";
  if (typeof afterRestart !== "string") {
    publishedAfterRestart = await publish(afterRestart, `homes/${enrolled}/devices/${enrolled}-bat-1/telemetry`, reading(`${enrolled}-bat-1`, 4));
    await afterRestart.endAsync().catch(() => undefined);
  }

  // Ingest may be working through a backlog (right after another scenario, say), so wait for what must arrive
  // (the enrolled gateway's first reading and the foreign-device dead letter) instead of a fixed pause, then a little
  // longer for the finding (S7-11: a fixed 3 s once judged a reading that arrived later as missing).
  const outcome = async () => (await pool.query<{ impersonated: number; foreign: number; enrolled_seqs: number[] | null; after_restart: number[] | null }>(`
    SELECT (SELECT count(*) FROM readings WHERE device_id = 'sim-gw-0002-bat-3' AND seq IN ($1, $2))::int AS impersonated,
           (SELECT count(*) FROM dead_letters WHERE reason = 'foreign_device' AND topic = 'homes/sim-gw-0001/devices/sim-gw-0002-bat-3/telemetry'
              AND convert_from(payload, 'UTF8')::jsonb->>'seq' = $2::text)::int AS foreign,
           (SELECT array_agg(seq::int ORDER BY seq) FROM readings WHERE device_id = $3) AS enrolled_seqs,
           (SELECT array_agg(seq::int ORDER BY seq) FROM readings WHERE device_id = $3 AND received_at > $4) AS after_restart`,
    [marker, marker + 1, `${enrolled}-bat-1`, restartedAt])).rows[0];
  const deadline = Date.now() + 60_000;
  for (let seen = await outcome(); Date.now() < deadline; seen = await outcome()) {
    if ((seen?.enrolled_seqs ?? []).includes(1) && (seen?.foreign ?? 0) > 0) break;
    await sleep(1000);
  }
  await sleep(5000);
  const result = await outcome();
  await pool.end();
  const enrolledSeqs = result?.enrolled_seqs ?? [];
  const afterRestartSeqs = result?.after_restart ?? [];

  finish("scenario 6: impersonation and revocation", [
    { check: "cross-topic publish refused by the broker (ACL by certificate CN)", pass: /not authori[sz]ed|135/i.test(crossTopic), detail: crossTopic },
    { check: "a foreign device under the gateway's own topic is dead-lettered by ingest (D6 binding)", pass: ownTopicForeignDevice === "accepted" && result?.foreign === 1, detail: `broker: ${ownTopicForeignDevice}; foreign_device dead letters: ${result?.foreign}` },
    { check: "nothing impersonated was stored", pass: result?.impersonated === 0, detail: `${result?.impersonated} readings` },
    { check: "a freshly enrolled gateway works before revocation", pass: beforeRevocation === "accepted" && enrolledSeqs.includes(1), detail: `publish ${beforeRevocation}, stored seqs ${JSON.stringify(enrolledSeqs)}` },
    { check: "after revocation + SIGHUP, the gateway can't connect", pass: refusedAfterRevocation, detail: refusedAfterRevocation ? `refused: ${reconnect}` : "connected" },
    {
      check: "the connection open before the CRL reload keeps publishing until it's dropped (S4-3)", pass: true,
      known: `seq 2 on the open connection: publish ${openConnectionAfterRevocation}, stored ${enrolledSeqs.includes(2)}`,
      detail: "the CRL is checked only at the handshake; the runbook's next step drops the connection",
    },
    {
      check: "after the runbook's broker restart: the old connection is gone and a new handshake is refused as revoked",
      pass: oldConnectionAfterRestart !== "accepted" && refusedAfterRestart,
      detail: `old connection seq 3: ${oldConnectionAfterRestart}; new handshake: ${typeof afterRestart === "string" ? afterRestart.trim().slice(0, 120) : `connected, seq 4 ${publishedAfterRestart}`}`,
    },
    {
      check: "nothing from the revoked gateway is stored after the restart",
      pass: afterRestartSeqs.length === 0,
      detail: `stored seqs ${JSON.stringify(enrolledSeqs)}; received after the restart: ${JSON.stringify(afterRestartSeqs)}`,
    },
  ], {
    restartedAt: restartedAt.toISOString(), enrolledSeqs, afterRestartSeqs,
    openConnectionAfterRevocation, oldConnectionAfterRestart, publishedAfterRestart,
  });
}

await run(main);
