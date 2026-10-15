/**
 * Ingest's Postgres pool, and the one way ingest holds a connection from it (S7-10).
 *
 * A pooled client that loses its socket emits `error`. pg-pool listens for that only while the client is idle: it
 * removes its listener at checkout (`pg-pool/index.js:344`) and puts it back on release. An `error` event with no
 * listener throws, which kills the process, so a database that dropped a connection mid-batch used to restart ingest.
 * `pool.query()` guards its own short-lived client (`pg-pool/index.js:464`); a client from `pool.connect()` has no
 * guard unless the holder adds one, which is what {@link withClient} does.
 */
import pg from "pg";

/** How ingest's connections bound a database that is slow, or doesn't answer at all. */
export interface IngestPoolOptions {
  url: string;
  size: number;
  /**
   * Server side: Postgres cancels a statement running longer than this (SQLSTATE 57014, retryable), and the
   * connection stays usable. A 500-row batch commits in milliseconds, so this fires only on something really wrong.
   */
  statementTimeoutMs?: number;
  /**
   * Client side, for a database that can't answer at all (a blackholed network): the driver gives up with "Query read
   * timeout" (retryable). Longer than the statement timeout, so a server that is alive cancels first.
   */
  queryTimeoutMs?: number;
  /** See {@link TRANSACTION_TIMEOUT_MS}; for tests. */
  transactionTimeoutMs?: number;
}

/** Ingest's defaults; see {@link IngestPoolOptions}. */
export const STATEMENT_TIMEOUT_MS = 30_000;
export const QUERY_TIMEOUT_MS = 35_000;

/**
 * No batch transaction may last longer than this (Postgres 17 `transaction_timeout`, S7-11). The API's gap window
 * reads readings by `stored_at`, which is a transaction's *start*; it re-reads 90 s behind its cursor, and that is
 * only safe because a transaction can't commit later than this after it started. A batch that hits it is retried
 * like any other timeout (SQLSTATE 25P04).
 */
export const TRANSACTION_TIMEOUT_MS = 60_000;

/**
 * Opens ingest's pool: a 5 s connect timeout, both query timeouts, and TCP keepalive so a dead peer is noticed even
 * while a connection sits idle. Idle-client errors are handed to `onIdleError` (pg-pool's own `error` event).
 */
export function openIngestPool(options: IngestPoolOptions, onIdleError: (error: Error) => void): pg.Pool {
  const pool = new pg.Pool({
    connectionString: options.url,
    max: options.size,
    connectionTimeoutMillis: 5000,
    statement_timeout: options.statementTimeoutMs ?? STATEMENT_TIMEOUT_MS,
    query_timeout: options.queryTimeoutMs ?? QUERY_TIMEOUT_MS,
    options: `-c transaction_timeout=${options.transactionTimeoutMs ?? TRANSACTION_TIMEOUT_MS}`,
    keepAlive: true,
  });
  pool.on("error", onIdleError);
  return pool;
}

/**
 * Runs `fn` on a connection from `pool`, and owns that connection's `error` listener and its release:
 * - while `fn` holds the client, an `error` event is swallowed: the query in flight already rejects with the same
 *   error (pg's `_errorAllQueries`), so the failure reaches the caller's retry path instead of crashing the process;
 * - after a failure the client is released with the error, so the pool destroys it instead of handing a broken
 *   connection to the retry. Destroying it also aborts any open transaction on the server.
 */
export async function withClient<T>(pool: pg.Pool, fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  const swallow = () => undefined;
  client.on("error", swallow);
  try {
    const result = await fn(client);
    client.removeListener("error", swallow);
    client.release();
    return result;
  } catch (error) {
    client.removeListener("error", swallow);
    client.release(error instanceof Error ? error : new Error(String(error)));
    throw error;
  }
}
