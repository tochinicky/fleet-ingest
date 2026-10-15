/**
 * The API's Postgres pool (S7-10). Every query here is a request or a health-job step, so a hung one must give up
 * rather than hold a pool slot: with five slots, a few stuck gap scans would freeze the fleet metrics, and the alerts
 * built on them would go quiet. The bounds are tighter than ingest's, which protect a batch rather than a request.
 */
import pg from "pg";

/** Server side: Postgres cancels a statement running longer than this (SQLSTATE 57014). */
export const STATEMENT_TIMEOUT_MS = 10_000;
/** Client side, for a database that can't answer at all: the driver gives up with "Query read timeout". */
export const QUERY_TIMEOUT_MS = 15_000;

/** Opens the API's pool: both timeouts, a 5 s connect timeout, and TCP keepalive. */
export function openApiPool(url: string, onIdleError: (error: Error) => void): pg.Pool {
  const pool = new pg.Pool({
    connectionString: url,
    max: 5,
    connectionTimeoutMillis: 5000,
    statement_timeout: STATEMENT_TIMEOUT_MS,
    query_timeout: QUERY_TIMEOUT_MS,
    keepAlive: true,
  });
  pool.on("error", onIdleError);
  return pool;
}

/**
 * True if `error` means the database didn't answer in time: a statement cancelled by `statement_timeout` (57014),
 * a query that hit `query_timeout`, or no free connection within the connect timeout. Those are the database being
 * slow or away, so a request gets 503 rather than 500.
 */
export function isDatabaseTimeout(error: unknown): boolean {
  if (typeof error !== "object" || error === null) return false;
  if ("code" in error && error.code === "57014") return true;
  const message = error instanceof Error ? error.message : "";
  return /^(Query read timeout|timeout exceeded when trying to connect)$/.test(message);
}
