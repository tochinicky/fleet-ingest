/**
 * The three kinds of failure ingest distinguishes, and the rule that maps a database error onto one of them.
 * - retryable: the same work will succeed later (the database is down, restarting, or reported a conflict).
 * - poison: this one message can never be stored; dead-letter it and move on.
 * - fatal: the process itself is wrong (a bug, a schema mismatch); stop and let the supervisor restart it.
 */

/** Which of the three kinds a failure is. */
export type FailureKind = "retryable" | "poison" | "fatal";

/** A failure that already knows its kind, so callers branch on `kind` instead of on error classes or message text. */
export class IngestError extends Error {
  override name = "IngestError";
  readonly kind: FailureKind;

  /** Wraps `cause` with the kind it was classified as. */
  constructor(kind: FailureKind, message: string, cause?: unknown) {
    super(message, { cause });
    this.kind = kind;
  }
}

/**
 * SQLSTATE classes and codes that mean "try again later": connection loss, shutdown, overload, conflicts, 57014 (a
 * statement cancelled by `statement_timeout`, S7-10), and 25P04 (a transaction ended by `transaction_timeout`, S7-11).
 */
const RETRYABLE_SQLSTATE = /^(08|53|57P0[1-3]|57014|25P04|40001|40P01)/;

/** SQLSTATE classes that mean "this row's data is unacceptable": data exceptions (22) and constraint violations (23). */
const POISON_SQLSTATE = /^(22|23)/;

/** Node socket error codes that mean the database couldn't be reached. */
const NETWORK_ERROR_CODES = new Set(["ECONNREFUSED", "ECONNRESET", "ETIMEDOUT", "EPIPE", "ENOTFOUND", "EAI_AGAIN"]);

/**
 * Messages the driver uses for a lost or unreachable database, when there's no code (S7-9). Each one is listed
 * with its source:
 * - `Connection terminated` / `… unexpectedly` / `… due to connection timeout` (pg `client.js`, pg-pool `index.js`);
 * - `timeout exceeded when trying to connect`: pg-pool, when no connection is ready within `connectionTimeoutMillis`;
 * - `timeout expired`: pg `client.js`, a connect that didn't complete in time;
 * - `Client has encountered a connection error`: pg, a query on a client whose socket already failed;
 * - `Query read timeout`: pg `client.js`, a query that got no answer within `query_timeout` (S7-10).
 */
const CONNECTION_LOSS_MESSAGE = /Connection terminated|timeout exceeded when trying to connect|timeout expired|connection timeout|Client has encountered a connection error|Query read timeout/i;

/**
 * Postgres's message for a row with no partition to go to (SQLSTATE 23514, which is otherwise a constraint
 * violation and so poison). It can't happen while partition maintenance keeps its window (S8-2); if it ever does,
 * the batch is retried until maintenance has created the partition, never dead-lettered, so nothing is lost
 * silently, and the retries alert (DBWriteErrors).
 */
const NO_PARTITION_MESSAGE = /no partition of relation "readings" found for row/;

/** How deep `cause` chains are followed; the driver wraps at most once or twice. */
const MAX_CAUSE_DEPTH = 5;

/** The network code of `error` or of any error in its `cause` chain, if one is a known network failure. */
function networkCode(error: unknown, depth = 0): string | undefined {
  if (typeof error !== "object" || error === null || depth > MAX_CAUSE_DEPTH) return undefined;
  const code = "code" in error ? String(error.code) : undefined;
  if (code && NETWORK_ERROR_CODES.has(code)) return code;
  return "cause" in error ? networkCode(error.cause, depth + 1) : undefined;
}

/**
 * Classifies an error thrown by the Postgres driver. Node network codes anywhere in the `cause` chain, and the
 * driver's connection-loss messages, are retryable. A five-character `code` is a SQLSTATE. Everything else is fatal,
 * because an unknown failure must not be retried forever or silently dead-lettered; "fatal" means the process exits
 * and `restart: always` brings it back, so a misclassified outage is a restart loop, not lost data (S7-9).
 */
export function classifyDatabaseError(error: unknown): FailureKind {
  if (networkCode(error)) return "retryable";
  if (NO_PARTITION_MESSAGE.test(error instanceof Error ? error.message : "")) return "retryable";
  const code = typeof error === "object" && error !== null && "code" in error ? String(error.code) : undefined;
  if (code && code.length === 5) {
    if (RETRYABLE_SQLSTATE.test(code)) return "retryable";
    if (POISON_SQLSTATE.test(code)) return "poison";
    return "fatal";
  }
  const message = error instanceof Error ? error.message : "";
  if (CONNECTION_LOSS_MESSAGE.test(message)) return "retryable";
  return "fatal";
}
