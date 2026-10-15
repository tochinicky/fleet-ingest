/**
 * Starts, restarts, crashes and inspects throwaway Mosquitto containers for the slice 0 spike.
 * Each check gets its own container and config, so one check's queue or session state can't leak into another.
 */
import { execFileSync } from "node:child_process";
import { mkdirSync, rmSync, writeFileSync, chmodSync, statSync, existsSync } from "node:fs";
import { createServer } from "node:net";
import { join } from "node:path";

/** The exact broker image that D2 pins. */
export const IMAGE = "eclipse-mosquitto:2.1.2-alpine";

/** A running spike broker: its container name, host port and data directory. */
export interface Broker {
  name: string;
  port: number;
  url: string;
  dataDir: string;
}

/** Settings that every spike broker shares; checks append their own lines after these. */
const BASE_CONFIG = [
  "listener 1883",
  "allow_anonymous true",
  "persistence true",
  "persistence_location /mosquitto/data/",
  "sys_interval 1",
  "log_dest stdout",
  "log_type error",
  "log_type warning",
  "log_type notice",
  "log_type information",
  "log_timestamp_format %Y-%m-%dT%H:%M:%S",
];

/**
 * The persist-sqlite plugin (2.1.0+) instead of snapshots: it writes queued and in-flight messages to SQLite and
 * commits every `flush_period` seconds (default 5). It can't be combined with `persistence true`.
 */
const SQLITE_PERSISTENCE = ["global_plugin /usr/lib/mosquitto_persist_sqlite.so", "plugin_opt_flush_period 5"];

/** Runs the docker CLI and returns its trimmed stdout. Throws if docker exits non-zero. */
function docker(...args: string[]): string {
  return execFileSync("docker", args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
}

/** Asks the OS for a free TCP port, so a container keeps the same host port across restarts. */
async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.listen(0, () => {
      const address = server.address();
      server.close(() => (typeof address === "object" && address ? resolve(address.port) : reject(new Error("no port"))));
    });
  });
}

/** Waits until the broker logs that it's listening, failing after the timeout. */
async function waitForReady(name: string, timeoutMs = 15_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (logs(name).includes("running")) return;
    await sleep(200);
  }
  throw new Error(`broker ${name} did not start: ${logs(name).slice(-500)}`);
}

/** Resolves after the given number of milliseconds. */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Starts a fresh broker container named `spike-<name>`, with the base config plus `extraConfig`. `persistence` picks
 * snapshots (`persistence true` plus autosave, D3) or the persist-sqlite plugin (the slice 6 experiment).
 * Any previous container and data with that name are removed first, so every check starts from an empty broker.
 */
export async function startBroker(name: string, extraConfig: string[] = [], persistence: "snapshot" | "sqlite" = "snapshot"): Promise<Broker> {
  const container = `spike-${name}`;
  const dir = join(import.meta.dirname, "..", "data", name);
  removeBroker(container, dir);
  mkdirSync(join(dir, "data"), { recursive: true });
  chmodSync(join(dir, "data"), 0o777);
  const base = persistence === "snapshot" ? BASE_CONFIG : BASE_CONFIG.filter((line) => line !== "persistence true").concat(SQLITE_PERSISTENCE);
  writeFileSync(join(dir, "mosquitto.conf"), [...base, ...extraConfig, ""].join("\n"));
  const port = await freePort();
  docker(
    "run", "-d", "--name", container, "-p", `127.0.0.1:${port}:1883`,
    "-v", `${join(dir, "mosquitto.conf")}:/mosquitto/config/mosquitto.conf:ro`,
    "-v", `${join(dir, "data")}:/mosquitto/data`,
    IMAGE,
  );
  await waitForReady(container);
  return { name: container, port, url: `mqtt://127.0.0.1:${port}`, dataDir: join(dir, "data") };
}

/** Removes a container and its data directory if they exist. */
function removeBroker(container: string, dir: string): void {
  try {
    docker("rm", "-f", container);
  } catch {
    /* not running: nothing to remove */
  }
  if (existsSync(dir)) rmSync(dir, { recursive: true, force: true });
}

/** Stops the broker cleanly (SIGTERM, so it writes its persistence file) and starts it again. */
export async function restartCleanly(broker: Broker): Promise<void> {
  const before = logs(broker.name).length;
  docker("restart", "-t", "30", broker.name);
  await waitForReadyAfter(broker.name, before);
}

/** Kills the broker with SIGKILL, so it can't save, then starts it again: a crash or an OOM kill. */
export async function crashAndStart(broker: Broker): Promise<void> {
  const before = logs(broker.name).length;
  docker("kill", "-s", "KILL", broker.name);
  docker("start", broker.name);
  await waitForReadyAfter(broker.name, before);
}

/** Waits for a "running" log line written after a restart, ignoring older lines in the log. */
async function waitForReadyAfter(name: string, logOffset: number, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (logs(name).slice(logOffset).includes("running")) return;
    await sleep(200);
  }
  throw new Error(`broker ${name} did not come back`);
}

/** Sends SIGUSR1, which tells Mosquitto to write its in-memory database to disk now. */
export function triggerSnapshot(broker: Broker): void {
  docker("kill", "-s", "USR1", broker.name);
}

/** Returns the broker's whole log (stdout and stderr). */
export function logs(name: string): string {
  return execFileSync("sh", ["-c", `docker logs ${name} 2>&1`], { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 });
}

/** Reads the mosquitto process's current resident memory (VmRSS) in bytes from /proc inside the container. */
export function brokerRssBytes(broker: Broker): number {
  return procStatusBytes(broker, "VmRSS");
}

/** Reads the mosquitto process's peak resident memory since it started (VmHWM, the high-water mark) in bytes. */
export function brokerPeakRssBytes(broker: Broker): number {
  return procStatusBytes(broker, "VmHWM");
}

/** Reads one kB-valued field from the mosquitto process's /proc status and returns it in bytes. */
function procStatusBytes(broker: Broker, field: string): number {
  const status = docker("exec", broker.name, "sh", "-c", "cat /proc/$(pidof mosquitto)/status");
  const match = new RegExp(`${field}:\\s+(\\d+) kB`).exec(status);
  if (!match) throw new Error(`${field} not found`);
  return Number(match[1]) * 1024;
}

/** Size in bytes of the persistence file, or 0 if it hasn't been written yet. */
export function snapshotFileBytes(broker: Broker): number {
  const file = join(broker.dataDir, "mosquitto.db");
  return existsSync(file) ? statSync(file).size : 0;
}

/** Removes the container (data stays on disk for inspection until the next run). */
export function stopBroker(broker: Broker): void {
  try {
    docker("rm", "-f", broker.name);
  } catch {
    /* already gone */
  }
}
