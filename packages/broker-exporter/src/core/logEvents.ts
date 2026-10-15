/**
 * Recognises the two broker log lines that matter for alerting (decisions.md S6-3), observed on 2.1.2:
 * - "Outgoing messages are being dropped for client X." The queue bound was hit. It's logged once per episode, not
 *   per message, so it names *who*; the `$SYS` dropped counter says *how many* (S0-3).
 * - "Expiring client X due to timeout." A persistent session expired, and everything queued for it was discarded
 *   without being counted anywhere else (S0-7).
 * Pure: one log line in, an event or null out.
 */

/** What a log line reports. */
export type LogEvent = { kind: "dropping"; clientId: string } | { kind: "expiring"; clientId: string };

/** Classifies one line of Mosquitto's log; null for every other line. */
export function classifyLogLine(line: string): LogEvent | null {
  const dropping = /Outgoing messages are being dropped for client (\S+?)\.?$/.exec(line.trim());
  if (dropping?.[1]) return { kind: "dropping", clientId: dropping[1] };
  const expiring = /Expiring client (\S+) due to timeout/.exec(line);
  if (expiring?.[1]) return { kind: "expiring", clientId: expiring[1] };
  return null;
}
