/**
 * Canary for the database driver's connection-loss messages (decisions.md S7-9). `classifyDatabaseError` matches some
 * of them by text, so a pg or pg-pool release that rewords one would quietly turn an outage back into a restart loop.
 * Here the real driver, with no mocks, produces each error against a local TCP server that misbehaves the way an
 * unreachable or hung database does, and each must classify as retryable:
 * - a server that accepts and never answers: a client connect times out ("timeout expired");
 * - the same, through a pool of one whose only client is busy: the pool gives up ("timeout exceeded when trying to
 *   connect");
 * - a server that accepts and then drops the socket ("Connection terminated unexpectedly");
 * - a server that completes the startup handshake and then never answers a query: `query_timeout` gives up
 *   ("Query read timeout", S7-10).
 * The deliberate-break check `driver-message-reworded` rewords the pattern and requires this file to fail.
 */
import { createServer } from "node:net";
import type { AddressInfo, Server, Socket } from "node:net";
import pg from "pg";
import { afterEach, describe, expect, it } from "vitest";
import { classifyDatabaseError } from "../src/core/errors.ts";

let servers: Server[] = [];
let sockets: Socket[] = [];

afterEach(async () => {
  sockets.forEach((socket) => socket.destroy());
  await Promise.all(servers.map((server) => new Promise((resolve) => server.close(resolve))));
  servers = [];
  sockets = [];
});

/** Starts a local server that runs `onConnection` for every client; resolves with its connection string. */
async function misbehavingServer(onConnection: (socket: Socket) => void): Promise<string> {
  const server = createServer((socket) => {
    sockets.push(socket);
    onConnection(socket);
  });
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  return `postgres://fleet:x@127.0.0.1:${(server.address() as AddressInfo).port}/fleet`;
}

/** Runs `attempt` and returns the error it rejects with; fails the test if it resolves. */
async function errorOf(attempt: () => Promise<unknown>): Promise<unknown> {
  try {
    await attempt();
  } catch (error) {
    return error;
  }
  throw new Error("expected the driver to fail");
}

/**
 * Answers a client's startup message the way a trusting Postgres does (AuthenticationOk, then ReadyForQuery), and
 * after that answers nothing: a database that accepted the connection and then went silent.
 */
function acceptThenGoSilent(socket: Socket): void {
  socket.once("data", () => {
    const authenticationOk = Buffer.from([0x52, 0, 0, 0, 8, 0, 0, 0, 0]);
    const readyForQuery = Buffer.from([0x5a, 0, 0, 0, 5, 0x49]);
    socket.write(Buffer.concat([authenticationOk, readyForQuery]));
  });
}

describe("driver connection-loss messages (pg 8.23.0, pg-pool 3.14.0)", () => {
  it("a connect to a server that never answers: 'timeout expired', retryable", async () => {
    const url = await misbehavingServer(() => undefined);
    const client = new pg.Client({ connectionString: url, connectionTimeoutMillis: 200 });
    const error = await errorOf(() => client.connect());
    expect((error as Error).message).toMatch(/timeout expired/);
    expect(classifyDatabaseError(error)).toBe("retryable");
  });

  it("a pool of one: the waiting connect gets 'timeout exceeded when trying to connect', the first 'Connection terminated due to connection timeout'; both retryable", async () => {
    const url = await misbehavingServer(() => undefined);
    const pool = new pg.Pool({ connectionString: url, max: 1, connectionTimeoutMillis: 200 });
    pool.on("error", () => undefined);
    const first = errorOf(() => pool.connect());
    const waiting = await errorOf(() => pool.connect());
    const firstError = await first;
    await pool.end().catch(() => undefined);
    expect((waiting as Error).message).toBe("timeout exceeded when trying to connect");
    expect(classifyDatabaseError(waiting)).toBe("retryable");
    expect((firstError as Error).message).toBe("Connection terminated due to connection timeout");
    expect(classifyDatabaseError(firstError)).toBe("retryable");
  });

  it("a server that drops the socket after accepting: 'Connection terminated unexpectedly', retryable", async () => {
    const url = await misbehavingServer((socket) => socket.once("data", () => socket.destroy()));
    const client = new pg.Client({ connectionString: url, connectionTimeoutMillis: 2000 });
    client.on("error", () => undefined);
    const error = await errorOf(() => client.connect());
    expect((error as Error).message).toMatch(/Connection terminated unexpectedly/);
    expect(classifyDatabaseError(error)).toBe("retryable");
  });

  it("a query the connected server never answers: 'Query read timeout', retryable", async () => {
    const url = await misbehavingServer(acceptThenGoSilent);
    const client = new pg.Client({ connectionString: url, query_timeout: 200 });
    client.on("error", () => undefined);
    await client.connect();
    const error = await errorOf(() => client.query("SELECT 1"));
    expect((error as Error).message).toBe("Query read timeout");
    expect(classifyDatabaseError(error)).toBe("retryable");
  });
});
