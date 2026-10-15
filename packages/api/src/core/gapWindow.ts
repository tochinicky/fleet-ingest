/**
 * Open gaps over the last hour, kept up to date from what was stored since the last pass instead of recomputed from
 * the whole window each time (S7-11). Pure: the health job feeds it rows and asks it questions; the database access
 * is in the job.
 *
 * Why: the fleet-wide gap query reads every reading of the last hour (about 770k at 500 gateways), which took 6–9 s
 * on the lab VM whatever its shape, against a 10 s statement timeout. Its cost grew with fleet size × window. Here
 * the database work per pass grows only with the ingest rate (the rows stored since the last pass), and the window
 * lives in memory: about 12 bytes per reading in the window, ~17 MB for this lab's hour. That memory still grows with
 * fleet × window, which is why Stage 3's answer is a rollup table and time partitioning, not a bigger process.
 *
 * It means exactly what the old query meant ({@link gapsOf} is that meaning, written out): per (device, epoch), the
 * readings measured in [from, to], in seq order, and every step of more than one between neighbours. A reading that
 * arrives late (a replay, a reordered duplicate) fills its gap as soon as it's added; a reading that ages out of the
 * window takes the gaps at the window's edge with it.
 */
import type { Gap } from "./health.ts";

/** One stored reading, as far as gaps are concerned. Times are epoch milliseconds. */
export interface WindowReading {
  deviceId: string;
  gatewayId: string;
  epoch: number;
  seq: number;
  measuredAt: number;
}

/** A gap and the gateway it belongs to. */
export type GatewayGap = Gap & { gatewayId: string };

/** One (device, epoch): its seqs in ascending order, and each one's measured time at the same index. */
interface Series {
  deviceId: string;
  gatewayId: string;
  epoch: number;
  seqs: number[];
  measuredAt: number[];
}

/** The index of the first element of sorted `values` that is ≥ `value`. */
function lowerBound(values: number[], value: number): number {
  let low = 0;
  let high = values.length;
  while (low < high) {
    const mid = (low + high) >>> 1;
    if ((values[mid] as number) < value) low = mid + 1;
    else high = mid;
  }
  return low;
}

/** Orders gaps the way the SQL does: by gateway, then by the time of the reading before the gap. */
function byGatewayThenBefore(a: GatewayGap, b: GatewayGap): number {
  if (a.gatewayId !== b.gatewayId) return a.gatewayId < b.gatewayId ? -1 : 1;
  return a.before.getTime() - b.before.getTime();
}

/** The gaps among `series`'s readings measured in [from, to], appended to `out`. */
function gapsInSeries(series: Series, from: number, to: number, out: GatewayGap[]): void {
  let previous = -1;
  for (let i = 0; i < series.seqs.length; i++) {
    const measuredAt = series.measuredAt[i] as number;
    if (measuredAt < from || measuredAt > to) continue;
    if (previous >= 0) {
      const prevSeq = series.seqs[previous] as number;
      const seq = series.seqs[i] as number;
      if (seq - prevSeq > 1) {
        out.push({
          deviceId: series.deviceId, gatewayId: series.gatewayId, epoch: series.epoch,
          fromSeq: prevSeq + 1, toSeq: seq - 1, missing: seq - prevSeq - 1, before: new Date(series.measuredAt[previous] as number),
        });
      }
    }
    previous = i;
  }
}

/** The readings of the window, per (device, epoch), and the open gaps among them. */
export class GapWindow {
  private readonly series = new Map<string, Series>();
  private readings = 0;

  /**
   * Adds stored readings. Idempotent: a reading already held (the same device, epoch and seq; re-read through the
   * cursor overlap) changes nothing. Readings measured before `from` can never be in the window again, and are
   * skipped.
   */
  add(rows: Iterable<WindowReading>, from: number): void {
    for (const row of rows) {
      if (row.measuredAt < from) continue;
      const key = `${row.deviceId}\u0000${row.epoch}`;
      let series = this.series.get(key);
      if (!series) {
        series = { deviceId: row.deviceId, gatewayId: row.gatewayId, epoch: row.epoch, seqs: [], measuredAt: [] };
        this.series.set(key, series);
      }
      const at = lowerBound(series.seqs, row.seq);
      if (series.seqs[at] === row.seq) continue;
      series.seqs.splice(at, 0, row.seq);
      series.measuredAt.splice(at, 0, row.measuredAt);
      this.readings++;
    }
  }

  /** Forgets readings measured before `from`: the window has moved past them. Compacts in place, allocating nothing. */
  prune(from: number): void {
    for (const [key, series] of this.series) {
      let kept = 0;
      for (let i = 0; i < series.seqs.length; i++) {
        const measuredAt = series.measuredAt[i] as number;
        if (measuredAt < from) continue;
        series.seqs[kept] = series.seqs[i] as number;
        series.measuredAt[kept] = measuredAt;
        kept++;
      }
      this.readings -= series.seqs.length - kept;
      series.seqs.length = kept;
      series.measuredAt.length = kept;
      if (kept === 0) this.series.delete(key);
    }
  }

  /** The open gaps among readings measured in [from, to], ordered like the SQL query's. */
  openGaps(from: number, to: number): GatewayGap[] {
    const out: GatewayGap[] = [];
    for (const series of this.series.values()) gapsInSeries(series, from, to, out);
    return out.sort(byGatewayThenBefore);
  }

  /** How many readings the window holds: its memory, roughly 12 bytes each. */
  size(): number {
    return this.readings;
  }
}

/**
 * The meaning of the fleet gap query, written out directly: the oracle that {@link GapWindow} must agree with, and
 * that an integration test checks against the SQL itself. `readings` must hold each (device, epoch, seq) once, as
 * the primary key guarantees.
 */
export function gapsOf(readings: WindowReading[], from: number, to: number): GatewayGap[] {
  const grouped = new Map<string, WindowReading[]>();
  for (const r of readings) {
    if (r.measuredAt < from || r.measuredAt > to) continue;
    const key = `${r.deviceId}\u0000${r.epoch}`;
    grouped.set(key, [...(grouped.get(key) ?? []), r]);
  }
  const out: GatewayGap[] = [];
  for (const rows of grouped.values()) {
    const sorted = [...rows].sort((a, b) => a.seq - b.seq);
    for (let i = 1; i < sorted.length; i++) {
      const prev = sorted[i - 1] as WindowReading;
      const cur = sorted[i] as WindowReading;
      if (cur.seq - prev.seq > 1) {
        out.push({ deviceId: cur.deviceId, gatewayId: cur.gatewayId, epoch: cur.epoch, fromSeq: prev.seq + 1, toSeq: cur.seq - 1, missing: cur.seq - prev.seq - 1, before: new Date(prev.measuredAt) });
      }
    }
  }
  return out.sort(byGatewayThenBefore);
}

/**
 * How far back each pass re-reads, behind the cursor: longer than any ingest transaction can last.
 *
 * The cursor is the newest `stored_at` seen. `stored_at` defaults to `now()`, which is the **transaction's start**,
 * not its commit. So a batch that started before another but committed after it carries an older `stored_at` than
 * rows the cursor has already passed. Read strictly after the cursor, those rows would be skipped forever, and their
 * gaps would stay open (or their fills unseen) until they aged out. Re-reading `stored_at > cursor − overlap` catches
 * every transaction that lasted less than the overlap, and re-read rows change nothing ({@link GapWindow.add} is
 * idempotent).
 *
 * Clocks: everything on the cursor path is the database's clock. The cursor starts at the database's `now()` and
 * moves only by `stored_at` values it read, and a pass's read has no upper bound. The window's own bounds (`from`,
 * `to`, and the prune cutoff) are compared with `measured_at`, which is the gateway's clock, not the database's or
 * the API's. They use the API's `now` because the old query did, so the result stays the same as the query's.
 *
 * The overlap is only safe if transactions really are bounded. Ingest's connections set `transaction_timeout` to
 * 60 s (Postgres 17), so no batch can commit more than 60 s after it started; the overlap adds 30 s of margin.
 */
export const CURSOR_OVERLAP_MS = 90_000;

/** The lower bound for a pass's read (`stored_at > bound`), or null to read everything (no cursor yet). */
export function deltaLowerBound(cursor: number | null, overlapMs = CURSOR_OVERLAP_MS): number | null {
  return cursor === null ? null : cursor - overlapMs;
}

/** The cursor after a pass: the newest `stored_at` seen so far. */
export function advanceCursor(cursor: number | null, storedAts: Iterable<number>): number | null {
  let next = cursor;
  for (const at of storedAts) if (next === null || at > next) next = at;
  return next;
}
