/**
 * Ingest's pool bounds a slow or silent database (S7-10). These options are easy to lose in a refactor and nothing
 * else would notice until a hang held a batch forever, so they're pinned here; the deliberate-break check
 * `database-timeouts-removed` deletes them and requires this file to fail.
 *
 * It also proves that a connection whose query timed out is destroyed and replaced, never reused: a reused client
 * would still have the stuck query ahead of the next one.
 */
import { createServer } from "node:net";
import type { AddressInfo, Socket } from "node:net";
import { describe, expect, it } from "vitest";
import { openIngestPool, QUERY_TIMEOUT_MS, STATEMENT_TIMEOUT_MS, TRANSACTION_TIMEOUT_MS, withClient } from "../src/adapters/databasePool.ts";

describe("openIngestPool", () => {
  it("sets the statement timeout, a longer query timeout, the transaction timeout (S7-11), TCP keepalive and the connect timeout", async () => {
    const pool = openIngestPool({ url: "postgres://fleet:x@127.0.0.1:1/fleet", size: 2 }, () => undefined);
    const options = pool.options;
    await pool.end();
    expect(options).toMatchObject({
      statement_timeout: 30_000, query_timeout: 35_000, options: "-c transaction_timeout=60000", keepAlive: true, connectionTimeoutMillis: 5000, max: 2,
    });
    expect(QUERY_TIMEOUT_MS).toBeGreaterThan(STATEMENT_TIMEOUT_MS);
    // The API's gap cursor re-reads 90 s behind (CURSOR_OVERLAP_MS): it relies on no transaction outliving this.
    expect(TRANSACTION_TIMEOUT_MS).toBeLessThanOrEqual(60_000);
  });
});

describe("withClient after a query timeout", () => {
  it("destroys the connection (the socket closes, the pool is empty) and the next use opens a fresh one", async () => {
    const sockets: Socket[] = [];
    const closed: Socket[] = [];
    // Answers the startup handshake (AuthenticationOk, ReadyForQuery) and then never answers a query.
    const server = createServer((socket) => {
      sockets.push(socket);
      socket.on("close", () => closed.push(socket));
      socket.on("error", () => undefined);
      socket.once("data", () => socket.write(Buffer.from([0x52, 0, 0, 0, 8, 0, 0, 0, 0, 0x5a, 0, 0, 0, 5, 0x49])));
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const url = `postgres://fleet:x@127.0.0.1:${(server.address() as AddressInfo).port}/fleet`;
    const pool = openIngestPool({ url, size: 1, queryTimeoutMs: 200 }, () => undefined);

    const first = await withClient(pool, (client) => client.query("SELECT 1")).then(() => undefined, (e: unknown) => e);
    await new Promise((resolve) => setTimeout(resolve, 100));
    const afterFirst = { total: pool.totalCount, opened: sockets.length, closed: closed.length };
    const second = await withClient(pool, (client) => client.query("SELECT 1")).then(() => undefined, (e: unknown) => e);
    await new Promise((resolve) => setTimeout(resolve, 100));
    const afterSecond = { total: pool.totalCount, opened: sockets.length, closed: closed.length };
    await pool.end();
    sockets.forEach((socket) => socket.destroy());
    await new Promise((resolve) => server.close(resolve));

    expect((first as Error).message).toBe("Query read timeout");
    expect(afterFirst).toEqual({ total: 0, opened: 1, closed: 1 });
    expect((second as Error).message).toBe("Query read timeout");
    expect(afterSecond).toEqual({ total: 0, opened: 2, closed: 2 });
  });
});
