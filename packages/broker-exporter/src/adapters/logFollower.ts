/**
 * Follows a log file the way `tail -F` does: it starts at the end, and every second reads whatever was appended, line
 * by line. If the file shrinks (rotated or truncated), it starts again from the beginning. The broker writes its log
 * to a volume this service mounts read-only.
 */
import { closeSync, existsSync, openSync, readSync, statSync } from "node:fs";

/**
 * Polls `path` every `intervalMs` and calls `onLine` for each complete new line. A read that fails (no permission,
 * file briefly missing during rotation) goes to `onError` and is retried on the next poll, so it never crashes the
 * exporter. Returns a function that stops it.
 */
export function followLog(path: string, onLine: (line: string) => void, onError: (error: unknown) => void, intervalMs = 1000): () => void {
  let offset = -1;
  let partial = "";
  const timer = setInterval(() => {
    try {
      if (!existsSync(path)) return;
      const size = statSync(path).size;
      if (offset < 0 || size < offset) offset = offset < 0 ? size : 0;
      if (size === offset) return;
      const text = readRange(path, offset, size);
      offset = size;
      const lines = (partial + text).split("\n");
      partial = lines.pop() ?? "";
      lines.forEach(onLine);
    } catch (error) {
      onError(error);
    }
  }, intervalMs);
  return () => clearInterval(timer);
}

/** Reads bytes [from, to) of a file as UTF-8. */
function readRange(path: string, from: number, to: number): string {
  const buffer = Buffer.alloc(to - from);
  const fd = openSync(path, "r");
  try {
    readSync(fd, buffer, 0, buffer.length, from);
  } finally {
    closeSync(fd);
  }
  return buffer.toString("utf8");
}
