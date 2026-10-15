/**
 * A queue that runs async work one item at a time, in the order it was added (decisions.md S7-1). The command
 * reconciler puts every publish through one, so a pass that read an older desired mode can't publish it after the
 * endpoint published a newer one. Pure: no I/O, no clock.
 */

/** Runs work items one after another. */
export interface SerialQueue {
  /** Adds `work` to the end of the queue and resolves or rejects with its result once it has run. */
  run<T>(work: () => Promise<T>): Promise<T>;
}

/** Creates an empty queue. A failed item rejects its own promise and doesn't stop the items after it. */
export function createSerialQueue(): SerialQueue {
  let tail: Promise<unknown> = Promise.resolve();
  return {
    run: <T>(work: () => Promise<T>): Promise<T> => {
      const result = tail.then(work);
      tail = result.catch(() => undefined);
      return result;
    },
  };
}
