/**
 * Integration test for the command reconciler (decisions.md S7-1) against real Postgres and Mosquitto. The broker runs
 * without persistence, so a restart is a fresh broker that has lost every retained command. The reconciler must
 * publish the desired modes again from Postgres when it reconnects, and a gateway that subscribes afterwards must get
 * its mode. The deliberate-break check `reconciler-not-on-connect` removes the reconnect pass and requires this to fail.
 */
import { createServer } from "node:net";
import { connect } from "mqtt";
import { GenericContainer, Wait } from "testcontainers";
import type { StartedTestContainer } from "testcontainers";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import type { StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { commandTopic, gatewayCommandSchema } from "@fleet-ingest/contract";
import { applyMigrations, loadMigrations, MIGRATIONS_DIR } from "@fleet-ingest/db";
import { startCommandPublisher } from "../src/commandPublisher.ts";
import type { CommandPublisher } from "../src/commandPublisher.ts";
import { startCommandReconciler } from "../src/commandReconciler.ts";
import type { CommandReconciler } from "../src/commandReconciler.ts";

let postgres: StartedPostgreSqlContainer;
let broker: StartedTestContainer;
let pool: pg.Pool;
let commands: CommandPublisher;
let reconciler: CommandReconciler;
let url: string;

/** Asks the OS for a free TCP port, so the broker keeps the same host port across its restart. */
async function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.listen(0, () => {
      const address = server.address();
      server.close(() => (typeof address === "object" && address ? resolve(address.port) : reject(new Error("no port"))));
    });
  });
}

/** Resolves when `predicate` is true, polling; fails the test after `ms`. */
async function waitFor(predicate: () => boolean, ms = 15_000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("timed out");
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

/**
 * Connects as a gateway, subscribes to its command topic, and returns the mode it receives within `ms`, parsed with
 * the contract's command schema as the gateways do. Null if nothing valid arrives.
 */
async function modeSeenBy(gatewayId: string, ms = 5_000): Promise<string | null> {
  const client = connect(url, { protocolVersion: 5, clientId: `${gatewayId}-${Date.now()}`, reconnectPeriod: 250 });
  client.on("error", () => undefined);
  let mode: string | null = null;
  client.on("message", (_topic, payload) => {
    const parsed = gatewayCommandSchema.safeParse(JSON.parse(payload.toString()));
    if (parsed.success) mode = parsed.data.mode;
  });
  await new Promise((resolve) => client.once("connect", resolve));
  await client.subscribeAsync(commandTopic(gatewayId), { qos: 1 });
  await waitFor(() => mode !== null, ms).catch(() => undefined);
  await client.endAsync();
  return mode;
}

beforeAll(async () => {
  postgres = await new PostgreSqlContainer("postgres:17.11-alpine").start();
  pool = new pg.Pool({ connectionString: postgres.getConnectionUri() });
  const client = await pool.connect();
  await applyMigrations(client, loadMigrations(MIGRATIONS_DIR));
  client.release();
  await pool.query(`
    INSERT INTO gateways (gateway_id, first_seen, mode, desired_mode, desired_at) VALUES
      ('gw-1', now(), 'surplus', 'max', now()), ('gw-2', now(), 'off', 'off', now()), ('gw-3', now(), 'surplus', NULL, NULL)`);
  const port = await freePort();
  broker = await new GenericContainer("eclipse-mosquitto:2.1.2-alpine")
    .withCopyContentToContainer([{ content: "listener 1883\nallow_anonymous true\nlog_dest stdout\n", target: "/mosquitto/config/mosquitto.conf" }])
    .withExposedPorts({ container: 1883, host: port })
    .withWaitStrategy(Wait.forLogMessage(/running/))
    .start();
  url = `mqtt://${broker.getHost()}:${port}`;
  commands = await startCommandPublisher({ url, clientId: "api" });
  reconciler = startCommandReconciler({ pool, commands, onError: () => {}, intervalMs: 3600_000 });
});

afterAll(async () => {
  reconciler?.stop();
  await commands?.close();
  await pool?.end();
  await broker?.stop();
  await postgres?.stop();
});

describe("command reconciler", () => {
  it("publishes every desired mode when it connects, so gateways get them retained", async () => {
    await waitFor(() => commands.connected());
    expect(await modeSeenBy("gw-1")).toBe("max");
    expect(await modeSeenBy("gw-2")).toBe("off");
    expect(await modeSeenBy("gw-3", 1_500)).toBeNull();
  });

  it("restores the commands on a fresh broker, which lost every retained message", async () => {
    let reconnected = false;
    commands.onConnect(() => (reconnected = true));
    await broker.restart();
    await waitFor(() => reconnected, 30_000);
    expect(await modeSeenBy("gw-1")).toBe("max");
    expect(await modeSeenBy("gw-2")).toBe("off");
  });
});
