/**
 * Scenario 6: impersonation and revocation, against the running mTLS stack.
 * 1. With its own valid certificate, a gateway publishes on another gateway's topic. The broker refuses it (ACL by
 *    certificate CN), and nothing is stored.
 * 2. A gateway publishes, under its own topic, a reading for another gateway's device. The broker allows it, because
 *    the topic is its own, but ingest's device binding dead-letters it as `foreign_device` (defence in depth).
 * 3. A gateway is enrolled with scripts/gateway-admin.ts's steps (certificate and dynsec client), connects, and its
 *    reading (seq 1) is stored. It is then revoked with the same script's procedure (S8-5): certificate and CRL,
 *    SIGHUP, a handshake seen to be refused as revoked, then the dynsec kick. Judged:
 *    - the CRL reload was confirmed before the kick (a kick first would let it straight back in);
 *    - **the open connection is closed within 5 s of the kick**, without a broker restart: S4-3's KNOWN LIMIT, now a
 *      PASS check, with the latency reported;
 *    - a new handshake is refused as revoked, and nothing from the gateway is stored after the kick (seqs 2, 3).
 *
 * Usage: `node scripts/scenarios/impersonation.ts`. It brings the stack up itself if it isn't (S7-11: run first after
 * a `compose down`, it used to fail on its own setup).
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { connect } from "mqtt";
import type { MqttClient } from "mqtt";
import { enrolGateway, revokeGateway } from "../gatewayAdmin.ts";
import { stackEnrolment, stackRevocation } from "../stackAdmin.ts";
import { ensureFleet, finish, openDatabase, progress, run, sleep } from "./lib.ts";

/** How soon after the kick the revoked gateway's open connection must be gone (S8-5). */
const KICK_BUDGET_MS = 5000;

const ROOT = join(import.meta.dirname, "..", "..");
const CERTS = join(ROOT, "deploy", "certs");
/** fleet-sim's gateways have their certificates in their own directory, the one fleet-sim mounts (S8-5). */
const FLEET_CERTS = join(CERTS, "fleet");

/** Where `cn`'s certificate lives: the simulated fleet's, or the enrolled gateways' beside the CA. */
const certsFor = (cn: string): string => (cn.startsWith("sim-gw-") ? FLEET_CERTS : CERTS);

/** Connects as the certificate `cn`; resolves with the client, or with the refusal message. */
async function connectAs(cn: string, clientId = cn): Promise<MqttClient | string> {
  const client = connect("mqtts://127.0.0.1:8883", {
    protocolVersion: 5, clientId, reconnectPeriod: 0, connectTimeout: 5000, rejectUnauthorized: true,
    ca: readFileSync(join(CERTS, "ca.crt")), cert: readFileSync(join(certsFor(cn), `${cn}.crt`)), key: readFileSync(join(certsFor(cn), `${cn}.key`)),
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
  await enrolGateway(stackEnrolment, enrolled);

  progress(started, "1: sim-gw-0001 publishes on sim-gw-0002's topic");
  const gw1 = await connectAs("sim-gw-0001", "scenario-gw1");
  if (typeof gw1 === "string") throw new Error(`sim-gw-0001 refused: ${gw1}`);
  const crossTopic = await publish(gw1, "homes/sim-gw-0002/devices/sim-gw-0002-bat-3/telemetry", reading("sim-gw-0002-bat-3", marker));

  progress(started, "2: sim-gw-0001 publishes sim-gw-0002's device under its own topic");
  const ownTopicForeignDevice = await publish(gw1, "homes/sim-gw-0001/devices/sim-gw-0002-bat-3/telemetry", reading("sim-gw-0002-bat-3", marker + 1));
  await gw1.endAsync();

  progress(started, `3: ${enrolled} enrols, publishes, is revoked with scripts/gateway-admin.ts's procedure (S8-5)`);
  const fresh = await connectAs(enrolled);
  if (typeof fresh === "string") throw new Error(`${enrolled} refused before revocation: ${fresh}`);
  const closedAt = new Promise<number>((resolve) => fresh.once("close", () => resolve(Date.now())));
  const beforeRevocation = await publish(fresh, `homes/${enrolled}/devices/${enrolled}-bat-1/telemetry`, reading(`${enrolled}-bat-1`, 1));
  const revocationStarted = Date.now();
  const times = await revokeGateway(stackRevocation, enrolled);
  // From the moment the kick was sent: the broker closes the connection before the admin's command returns.
  const kickedAt = new Date(revocationStarted + times.crlConfirmedMs);
  const closeMs = (await Promise.race([closedAt, sleep(KICK_BUDGET_MS * 2).then(() => Number.NaN)])) - kickedAt.getTime();
  const oldConnectionAfterKick = fresh.connected
    ? await publish(fresh, `homes/${enrolled}/devices/${enrolled}-bat-1/telemetry`, reading(`${enrolled}-bat-1`, 2))
    : "not connected";
  await fresh.endAsync(true).catch(() => undefined);
  const afterKick = await connectAs(enrolled, `${enrolled}-after-kick`);
  const refusedAfterKick = typeof afterKick === "string" && /revoked/i.test(afterKick);
  let publishedAfterKick = "no connection";
  if (typeof afterKick !== "string") {
    publishedAfterKick = await publish(afterKick, `homes/${enrolled}/devices/${enrolled}-bat-1/telemetry`, reading(`${enrolled}-bat-1`, 3));
    await afterKick.endAsync().catch(() => undefined);
  }

  // Ingest may be working through a backlog (right after another scenario, say), so wait for what must arrive
  // (the enrolled gateway's first reading and the foreign-device dead letter) instead of a fixed pause, then a little
  // longer for the finding (S7-11: a fixed 3 s once judged a reading that arrived later as missing).
  const outcome = async () => (await pool.query<{ impersonated: number; foreign: number; enrolled_seqs: number[] | null; after_kick: number[] | null }>(`
    SELECT (SELECT count(*) FROM readings WHERE device_id = 'sim-gw-0002-bat-3' AND seq IN ($1, $2))::int AS impersonated,
           (SELECT count(*) FROM dead_letters WHERE reason = 'foreign_device' AND topic = 'homes/sim-gw-0001/devices/sim-gw-0002-bat-3/telemetry'
              AND convert_from(payload, 'UTF8')::jsonb->>'seq' = $2::text)::int AS foreign,
           (SELECT array_agg(seq::int ORDER BY seq) FROM readings WHERE device_id = $3) AS enrolled_seqs,
           (SELECT array_agg(seq::int ORDER BY seq) FROM readings WHERE device_id = $3 AND received_at > $4) AS after_kick`,
    [marker, marker + 1, `${enrolled}-bat-1`, kickedAt])).rows[0];
  const deadline = Date.now() + 60_000;
  for (let seen = await outcome(); Date.now() < deadline; seen = await outcome()) {
    if ((seen?.enrolled_seqs ?? []).includes(1) && (seen?.foreign ?? 0) > 0) break;
    await sleep(1000);
  }
  await sleep(5000);
  const result = await outcome();
  await pool.end();
  const enrolledSeqs = result?.enrolled_seqs ?? [];
  const afterKickSeqs = result?.after_kick ?? [];

  finish("scenario 6: impersonation and revocation", [
    { check: "cross-topic publish refused by the broker (ACL by certificate CN)", pass: /not authori[sz]ed|135/i.test(crossTopic), detail: crossTopic },
    { check: "a foreign device under the gateway's own topic is dead-lettered by ingest (D6 binding)", pass: ownTopicForeignDevice === "accepted" && result?.foreign === 1, detail: `broker: ${ownTopicForeignDevice}; foreign_device dead letters: ${result?.foreign}` },
    { check: "nothing impersonated was stored", pass: result?.impersonated === 0, detail: `${result?.impersonated} readings` },
    { check: "a freshly enrolled gateway works before revocation", pass: beforeRevocation === "accepted" && enrolledSeqs.includes(1), detail: `publish ${beforeRevocation}, stored seqs ${JSON.stringify(enrolledSeqs)}` },
    {
      check: "the CRL reload was confirmed (a handshake refused as revoked) before the kick", pass: times.crlConfirmedMs <= times.kickedMs,
      detail: `confirmed after ${(times.crlConfirmedMs / 1000).toFixed(1)} s (${times.probes} probe(s)), kicked at ${(times.kickedMs / 1000).toFixed(1)} s`,
    },
    {
      check: `the open connection is closed within ${KICK_BUDGET_MS / 1000} s of the kick, without a broker restart (D6 change, S8-5)`,
      pass: Number.isFinite(closeMs) && closeMs <= KICK_BUDGET_MS && oldConnectionAfterKick !== "accepted",
      detail: Number.isFinite(closeMs) ? `closed ${(closeMs / 1000).toFixed(2)} s after the kick; seq 2 on it: ${oldConnectionAfterKick}` : `still open ${(KICK_BUDGET_MS * 2) / 1000} s after the kick`,
    },
    {
      check: "after the kick, a new handshake is refused as revoked",
      pass: refusedAfterKick,
      detail: typeof afterKick === "string" ? afterKick.trim().slice(0, 120) : `connected, seq 3 ${publishedAfterKick}`,
    },
    {
      check: "nothing from the revoked gateway is stored after the kick",
      pass: afterKickSeqs.length === 0,
      detail: `stored seqs ${JSON.stringify(enrolledSeqs)}; received after the kick: ${JSON.stringify(afterKickSeqs)}`,
    },
  ], {
    kickedAt: kickedAt.toISOString(), crlConfirmedSeconds: times.crlConfirmedMs / 1000, kickedSeconds: times.kickedMs / 1000,
    closeSecondsAfterKick: Number.isFinite(closeMs) ? closeMs / 1000 : null, enrolledSeqs, afterKickSeqs, oldConnectionAfterKick, publishedAfterKick,
  });
}

await run(main);
