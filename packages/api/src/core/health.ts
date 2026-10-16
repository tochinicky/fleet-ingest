/**
 * The health rules, pure: given what ingest last heard from a gateway and its devices, and the time now,
 * decide whether the gateway is online, offline or unknown, which devices are stale, and which gaps the gateway's own
 * dropped-message counter explains, and whether a command the cloud set is still waiting to be applied.
 *
 * "Online" is evidence-based (decisions.md S2-2, S5-1, S5-6). A reading received in the last minute, after the latest
 * `offline` status, proves the gateway is alive, whatever its stored status says. That way neither a gateway clock
 * that runs behind nor a late, queued Last Will can make a working gateway look dead, and a dead one still shows
 * offline as soon as its Will arrives. All ages are measured on ingest's clock (`received_at`), never the gateway's.
 */

/** A reading received within this window proves the gateway is alive. */
export const READING_FRESH_MS = 60_000;

/** A gateway whose last status was `online` but that has been silent this long is offline: 3 × its 60 s heartbeat. */
export const STATUS_SILENCE_MS = 3 * 60_000;

/** A device of an online gateway with no reading for this long is stale. */
export const DEVICE_STALE_MS = 60_000;

/** The three states a gateway can be in; `unknown` is never folded into online or offline (S1-2). */
export type GatewayState = "online" | "offline" | "unknown";

/** What ingest last heard from a gateway. Every time is ingest's receive time. */
export interface GatewayEvidence {
  lastStatus: "online" | "offline" | null;
  lastStatusReceivedAt: Date | null;
  /** When ingest last received an `offline` status (usually the Last Will). */
  lastOfflineReceivedAt: Date | null;
  lastReadingReceivedAt: Date | null;
}

/** A gateway's state and the rule that decided it, for the API and the runbook. */
export interface GatewayHealth {
  state: GatewayState;
  reason: string;
}

/** Milliseconds from `then` to `now`, or Infinity if `then` is unknown. */
function age(then: Date | null, now: Date): number {
  return then ? now.getTime() - then.getTime() : Infinity;
}

/**
 * True if a reading proves the gateway is alive: received in the last 60 s, **and after the latest `offline` status**.
 * Readings sent before a link died arrive before its Last Will, so they don't keep a dead gateway online (S5-6).
 * Readings after a Will mean it came back, even if its clock is wrong.
 */
function readingProvesLife(evidence: GatewayEvidence, now: Date): boolean {
  const reading = evidence.lastReadingReceivedAt;
  if (!reading || age(reading, now) > READING_FRESH_MS) return false;
  return !evidence.lastOfflineReceivedAt || reading > evidence.lastOfflineReceivedAt;
}

/**
 * Classifies a gateway. The rules, in order:
 * 1. a reading received in the last 60 s, after the latest `offline` status → online, whatever the stored status says;
 * 2. no status ever → unknown (it may have been offline since before ingest's sessions existed, S1-2);
 * 3. last status `offline` (often the Last Will) → offline;
 * 4. last status `online`, but nothing heard for 3 minutes → offline (the Last Will was lost, e.g. in a broker restart);
 * 5. otherwise → online.
 */
export function classifyGateway(evidence: GatewayEvidence, now: Date): GatewayHealth {
  if (readingProvesLife(evidence, now)) return { state: "online", reason: "reading received in the last 60 s, after any offline status" };
  if (evidence.lastStatus === null) return { state: "unknown", reason: "no status received yet" };
  if (evidence.lastStatus === "offline") return { state: "offline", reason: "last status is offline" };
  const silentMs = Math.min(age(evidence.lastStatusReceivedAt, now), age(evidence.lastReadingReceivedAt, now));
  if (silentMs > STATUS_SILENCE_MS) return { state: "offline", reason: `nothing heard for ${Math.round(silentMs / 1000)} s` };
  return { state: "online", reason: "last status is online" };
}

/** True if the device should be sending but isn't: its gateway is online and it has had no reading for 60 s. */
export function isDeviceStale(gatewayState: GatewayState, lastReadingReceivedAt: Date | null, now: Date): boolean {
  return gatewayState === "online" && age(lastReadingReceivedAt, now) > DEVICE_STALE_MS;
}

/** A command not applied by a connected gateway this long after it could have been is stuck (S7-1). */
export const COMMAND_STUCK_MS = 5 * 60_000;

/** What the cloud asked a gateway for, what the gateway reports, and when it last came back online. */
export interface CommandEvidence {
  desiredMode: string | null;
  desiredAt: Date | null;
  reportedMode: string | null;
  cameBackAt: Date | null;
}

/** Whether a gateway's desired mode is still waiting to be applied, since when, and whether that is now an alert. */
export interface CommandStatus {
  pending: boolean;
  /** When the desired mode was set, while it's pending; null otherwise. */
  pendingSince: Date | null;
  stuck: boolean;
}

/**
 * Compares desired with reported state (S7-1). A desired mode is pending until the gateway's status reports it. It is
 * stuck only if the gateway is online and has had 5 minutes to apply it, counted from the later of when it was set
 * and when the gateway last came back: a gateway that was away can't be blamed for the time it was away. A time
 * that's unknown (NULL) is left out; with neither known, the age is unknown and counts as long.
 */
export function commandStatus(evidence: CommandEvidence, gatewayState: GatewayState, now: Date): CommandStatus {
  const pending = evidence.desiredMode !== null && evidence.desiredMode !== evidence.reportedMode;
  if (!pending) return { pending: false, pendingSince: null, stuck: false };
  const known = [evidence.desiredAt, evidence.cameBackAt].filter((t): t is Date => t !== null);
  const chance = known.length ? new Date(Math.max(...known.map((t) => t.getTime()))) : null;
  return { pending: true, pendingSince: evidence.desiredAt, stuck: gatewayState === "online" && age(chance, now) > COMMAND_STUCK_MS };
}

/** One gap in one device's sequence within one epoch: seqs `fromSeq`..`toSeq` never arrived. */
export interface Gap {
  deviceId: string;
  epoch: number;
  fromSeq: number;
  toSeq: number;
  missing: number;
  /** When the reading before the gap was measured: used to order a gateway's gaps oldest first. */
  before: Date;
}

/**
 * Marks which of a gateway's gaps its own counter explains. When the gateway's buffer overflows it drops
 * the oldest readings and counts them in `droppedTelemetry`, a **lifetime** counter persisted on the gateway.
 *
 * So `gaps` must be **all** of the gateway's gaps since that counter started: all time, all devices, all epochs. Only
 * then can the caller filter to a window for display. Comparing a lifetime counter with a window's gaps would
 * over-explain: 300 lifetime drops would cover every gap in any window.
 *
 * Gaps are assigned oldest first, and the assignment **stops at the first gap that doesn't fit** in the remaining
 * count. That gap and every later one are unexplained, even a later, smaller gap that would fit. Drops take the
 * oldest buffered readings, so an explained gap can't come after one the counter couldn't cover; skipping ahead
 * would explain gaps by drops that never happened there. This is a heuristic: the counter says how many were dropped,
 * not which.
 */
export function explainGaps(gaps: Gap[], droppedTelemetry: number): Array<Gap & { explained: boolean }> {
  return explainFrom(initialExplainState(droppedTelemetry), gaps).explained;
}

/** Where an oldest-first explanation stands: how much of the counter is left, and whether a gap has stopped it. */
export interface ExplainState {
  budget: number;
  stopped: boolean;
}

/** The state before any gap: the whole lifetime counter, not stopped. */
export function initialExplainState(droppedTelemetry: number): ExplainState {
  return { budget: Math.max(0, droppedTelemetry), stopped: false };
}

/**
 * The order gaps are explained in: oldest `before` first, then by device, epoch and seq, so that two gaps with the
 * same `before` (two devices measured in the same millisecond) are always taken in the same order. Without the
 * tie-break, a checkpointed explanation and a from-scratch one could take tied gaps in different orders and disagree
 * when the budget runs out between them (S8-4).
 */
export function compareGaps(a: Gap, b: Gap): number {
  return a.before.getTime() - b.before.getTime() || (a.deviceId < b.deviceId ? -1 : a.deviceId > b.deviceId ? 1 : 0)
    || a.epoch - b.epoch || a.fromSeq - b.fromSeq;
}

/**
 * {@link explainGaps}, resumable: explains `gaps` oldest first starting from `state`, and returns the state after
 * them. Explaining a prefix and then the rest from the returned state gives exactly the result of explaining both at
 * once, which is what lets a checkpoint stand in for the prefix (S8-4).
 */
export function explainFrom(state: ExplainState, gaps: Gap[]): { explained: Array<Gap & { explained: boolean }>; state: ExplainState } {
  let { budget, stopped } = state;
  const explained = [...gaps].sort(compareGaps).map((gap) => {
    const fits = !stopped && gap.missing <= budget;
    if (fits) budget -= gap.missing;
    else stopped = true;
    return { ...gap, explained: fits };
  });
  return { explained, state: { budget, stopped } };
}
