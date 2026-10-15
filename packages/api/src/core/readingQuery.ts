/**
 * Pure rules for reading-history requests: the default and maximum time window, the page size cap, and the keyset
 * cursor that lets a client page through a large window without OFFSET (which gets slower the deeper it goes).
 */
import { z } from "zod";

/** The widest window one request may ask for; a client wanting more pages through several windows. */
export const MAX_WINDOW_MS = 7 * 24 * 3600 * 1000;

/** The window used when the client gives no `from`. */
export const DEFAULT_WINDOW_MS = 3600 * 1000;

/** The most rows one page returns. */
export const MAX_PAGE_SIZE = 1000;

/** Where the previous page ended: the sort key of its last row. */
export interface Cursor {
  measuredAt: string;
  epoch: number;
  seq: number;
}

/** A validated request: a bounded window, a capped page size, and maybe a cursor to continue after. */
export interface ReadingQuery {
  from: Date;
  to: Date;
  limit: number;
  after: Cursor | null;
}

const cursorSchema = z.object({ measuredAt: z.iso.datetime(), epoch: z.number().int(), seq: z.number().int() });

const querySchema = z.object({
  from: z.iso.datetime({ offset: true }).optional(),
  to: z.iso.datetime({ offset: true }).optional(),
  limit: z.coerce.number().int().min(1).max(MAX_PAGE_SIZE).default(100),
  cursor: z.string().optional(),
});

/** Encodes a cursor as an opaque URL-safe string. */
export function encodeCursor(cursor: Cursor): string {
  return Buffer.from(JSON.stringify(cursor)).toString("base64url");
}

/** Decodes a cursor; returns null if it isn't one this API issued. */
export function decodeCursor(text: string): Cursor | null {
  try {
    const parsed = cursorSchema.safeParse(JSON.parse(Buffer.from(text, "base64url").toString("utf8")));
    return parsed.success ? parsed.data : null;
  } catch {
    return null;
  }
}

/**
 * Validates query-string parameters into a {@link ReadingQuery}. `to` defaults to `now`, `from` to an hour before `to`.
 * Returns an error message instead when the input is invalid, `from` is after `to`, or the window is wider than 7 days.
 */
export function parseReadingQuery(raw: unknown, now: Date): ReadingQuery | { error: string } {
  const parsed = querySchema.safeParse(raw);
  if (!parsed.success) return { error: parsed.error.issues.map((i) => `${i.path.join(".")}: ${i.message}`).join("; ") };
  const to = parsed.data.to ? new Date(parsed.data.to) : now;
  const from = parsed.data.from ? new Date(parsed.data.from) : new Date(to.getTime() - DEFAULT_WINDOW_MS);
  if (from > to) return { error: "from must not be after to" };
  if (to.getTime() - from.getTime() > MAX_WINDOW_MS) return { error: "the window may be at most 7 days" };
  const after = parsed.data.cursor === undefined ? null : decodeCursor(parsed.data.cursor);
  if (parsed.data.cursor !== undefined && after === null) return { error: "cursor is not valid" };
  return { from, to, limit: parsed.data.limit, after };
}
