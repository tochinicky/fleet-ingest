/**
 * Finds the bad row in a batch the database refused. When a batch write fails with a poison error
 * (a data or constraint error), the batch is split in half and each half is written on its own, recursively, until
 * the failing row is alone. That row goes to `onPoison` (which dead-letters it), and every other row is committed.
 * One bad row among n costs about 2·log₂(n) extra writes, instead of n writes one by one.
 *
 * Pure algorithm: the writes happen through the callbacks passed in. Retryable and fatal errors are not handled
 * here; they propagate, and the caller retries the whole batch. Halves already committed are written again, which is
 * harmless because every write is idempotent (D4).
 */
import { classifyDatabaseError, IngestError } from "./errors.ts";

/** How to write a batch, and what to do with a row that is poison on its own. */
export interface IsolationPorts<T, O> {
  /** Writes the items in one transaction and returns one outcome per item, in order. */
  write: (items: T[]) => Promise<O[]>;
  /** Handles a single item the database refused; returns its outcome (normally after dead-lettering it). */
  onPoison: (item: T, error: unknown) => Promise<O>;
}

/**
 * Writes `items`, isolating poison rows by halving. Returns one outcome per item, in the original order.
 * Throws an {@link IngestError} for any retryable or fatal error.
 */
export async function writeIsolatingPoison<T, O>(items: T[], ports: IsolationPorts<T, O>): Promise<O[]> {
  if (items.length === 0) return [];
  try {
    return await ports.write(items);
  } catch (error) {
    const kind = classifyDatabaseError(error);
    if (kind !== "poison") throw new IngestError(kind, "batch write failed", error);
    if (items.length === 1) return [await ports.onPoison(items[0] as T, error)];
    const middle = Math.ceil(items.length / 2);
    const left = await writeIsolatingPoison(items.slice(0, middle), ports);
    const right = await writeIsolatingPoison(items.slice(middle), ports);
    return [...left, ...right];
  }
}
