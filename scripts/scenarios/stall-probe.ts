/**
 * Stall probe (slice 6b, S6-5/S6-6): measures how responsive the broker and Postgres are, once a second, for a while,
 * under whatever load is running. Each second it times:
 * - a full mTLS MQTT connect (TCP, TLS handshake, CONNACK) to the broker with the `ops` certificate. The broker
 *   accepts connections on its single main loop, so this stalls exactly when the broker does;
 * - a `SELECT 1` against Postgres.
 * Optionally it also runs `docker stats` every 5 s, like the scenario scripts do, to see whether that causes stalls.
 * Prints the p50/p99/max of each, and every second where either took longer than 2 s.
 *
 * Usage: `node scripts/scenarios/stall-probe.ts <seconds> [--docker-stats]`
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { connect } from "mqtt";
import { containerMemoryMb, openDatabase, run, sleep } from "./lib.ts";

const CERTS = join(import.meta.dirname, "..", "..", "deploy", "certs");

/** Milliseconds for one full broker connect, or Infinity if it fails or takes over 30 s. */
async function brokerConnectMs(): Promise<number> {
  const started = performance.now();
  const client = connect("mqtts://127.0.0.1:8883", {
    protocolVersion: 5, clientId: `stall-probe-${Date.now()}`, reconnectPeriod: 0, connectTimeout: 30_000, rejectUnauthorized: true,
    ca: readFileSync(join(CERTS, "ca.crt")), cert: readFileSync(join(CERTS, "ops.crt")), key: readFileSync(join(CERTS, "ops.key")),
  });
  const ok = await new Promise<boolean>((resolve) => {
    client.once("connect", () => resolve(true));
    client.once("close", () => resolve(false));
  });
  const ms = performance.now() - started;
  client.end(true);
  return ok ? ms : Infinity;
}

/** The p-th percentile of `values` (0–100). */
function percentile(values: number[], p: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] ?? NaN;
}

/** Runs the probe and prints its summary. */
async function main(): Promise<void> {
  const seconds = Number(process.argv[2] ?? "120");
  const withDockerStats = process.argv.includes("--docker-stats");
  const pool = openDatabase();
  const broker: number[] = [];
  const database: number[] = [];
  const stalls: string[] = [];
  let statsTimer: NodeJS.Timeout | undefined;
  if (withDockerStats) statsTimer = setInterval(() => void Promise.all(["mosquitto", "ingest-1", "ingest-2"].map(containerMemoryMb)), 5000);
  const end = Date.now() + seconds * 1000;
  while (Date.now() < end) {
    const tick = Date.now();
    const [b, d] = await Promise.all([brokerConnectMs(), (async () => { const s = performance.now(); await pool.query("SELECT 1"); return performance.now() - s; })()]);
    broker.push(b);
    database.push(d);
    if (b > 2000 || d > 2000) stalls.push(`${new Date(tick).toISOString().slice(11, 19)} broker ${Math.round(b)} ms, db ${Math.round(d)} ms`);
    await sleep(Math.max(0, 1000 - (Date.now() - tick)));
  }
  if (statsTimer) clearInterval(statsTimer);
  await pool.end();
  const summary = (values: number[]) => ({ p50: Math.round(percentile(values, 50)), p99: Math.round(percentile(values, 99)), max: Math.round(Math.max(...values)) });
  process.stdout.write(`${JSON.stringify({ seconds, withDockerStats, brokerConnectMs: summary(broker), databaseMs: summary(database), stallsOver2s: stalls.length })}\n`);
  stalls.forEach((line) => process.stdout.write(`  stall ${line}\n`));
}

await run(main);
