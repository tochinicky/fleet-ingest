/**
 * The command reconciler (decisions.md S7-1): Postgres holds each gateway's desired mode, and the broker's retained
 * command is a copy that this module keeps in line with it. A fresh broker, a broker restart without persistence, or
 * a publish that failed while the broker was down all lose the copy; the reconciler publishes it again:
 * - on every connection to the broker: every desired mode, because the broker may have lost all of them;
 * - every 60 s: only the pending ones, whose gateway hasn't reported them yet. Re-publishing a retained command
 *   delivers it again to a subscribed gateway, so publishing all 500 every minute would send each gateway a command
 *   a minute for nothing.
 * Every publish, the endpoint's included, runs through one queue, so a pass that read an older desired mode can't
 * overwrite a newer one that the endpoint is publishing at the same moment.
 */
import type pg from "pg";
import type { GatewayMode } from "@fleet-ingest/contract";
import type { CommandPublisher } from "./commandPublisher.ts";
import { createSerialQueue } from "./core/serialQueue.ts";
import { desiredModes } from "./queries.ts";

/** How often pending commands are published again. */
export const RECONCILE_INTERVAL_MS = 60_000;

/** A publish gives up after this long, so a stalled broker can't hold the queue (or an HTTP request) forever. */
export const PUBLISH_TIMEOUT_MS = 5_000;

/** What the endpoint and the main process use. */
export interface CommandReconciler {
  /** Publishes one gateway's desired mode now, through the queue. Resolves true if the broker acknowledged it. */
  publish(gatewayId: string, mode: GatewayMode): Promise<boolean>;
  /** Runs one pass now: every desired mode, or only the pending ones. Resolves to how many were published. */
  reconcile(pendingOnly: boolean): Promise<number>;
  stop(): void;
}

/** Dependencies of the reconciler. */
export interface ReconcilerOptions {
  pool: pg.Pool;
  commands: CommandPublisher;
  onError: (error: unknown) => void;
  intervalMs?: number;
}

/** Rejects after `ms`, for racing against a publish. */
function timeout(ms: number): Promise<never> {
  return new Promise((_resolve, reject) => setTimeout(() => reject(new Error(`publish timed out after ${ms} ms`)), ms).unref());
}

/** Starts the reconciler: a full pass on every broker connection, a pending-only pass every interval. */
export function startCommandReconciler(options: ReconcilerOptions): CommandReconciler {
  const queue = createSerialQueue();
  /** Runs `work` in the reconciler's one queue, after everything already in it. */
  const serial = <T>(work: () => Promise<T>): Promise<T> => queue.run(work);

  /** Publishes one mode if the broker is connected; false if it isn't, or if the publish failed or timed out. */
  const publishNow = async (gatewayId: string, mode: GatewayMode): Promise<boolean> => {
    if (!options.commands.connected()) return false;
    try {
      await Promise.race([options.commands.publishMode(gatewayId, mode), timeout(PUBLISH_TIMEOUT_MS)]);
      return true;
    } catch (error) {
      options.onError(error);
      return false;
    }
  };

  /** Reads the desired modes and publishes them, inside the queue. */
  const reconcile = (pendingOnly: boolean): Promise<number> => serial(async () => {
    if (!options.commands.connected()) return 0;
    let published = 0;
    for (const desired of await desiredModes(options.pool, pendingOnly)) {
      if (await publishNow(desired.gatewayId, desired.mode as GatewayMode)) published++;
    }
    return published;
  });

  /** Runs a pass in the background, reporting a failure instead of throwing. */
  const background = (pendingOnly: boolean): void => void reconcile(pendingOnly).catch(options.onError);

  options.commands.onConnect(() => background(false));
  if (options.commands.connected()) background(false);
  const timer = setInterval(() => background(true), options.intervalMs ?? RECONCILE_INTERVAL_MS);

  return {
    publish: (gatewayId, mode) => serial(() => publishNow(gatewayId, mode)),
    reconcile,
    stop: () => clearInterval(timer),
  };
}
