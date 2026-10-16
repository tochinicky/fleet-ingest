/**
 * The smoke check's gateway identities (S8-8). Each run issues its own, `smoke-<UTC timestamp>`, so a run never
 * depends on an earlier run's certificate: the first version reused `smoke-gw-1`, and once the revocation test had
 * revoked it (as the runbook says to), every later smoke check failed. A revoked earlier identity is useful instead:
 * the check that a revoked certificate is refused.
 */

/** This run's gateway ID: `smoke-` and the UTC time to the second, lowercase (a valid CN, topic segment and username). */
export function smokeGatewayId(now: Date): string {
  return `smoke-${now.toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "z").toLowerCase()}`;
}

/**
 * The most recently revoked smoke identity in an `openssl ca` database (index.txt), or null. Lines are tab-separated:
 * status (V, R, E), expiry, revocation time, serial, file, subject.
 */
export function lastRevokedSmokeId(indexTxt: string): string | null {
  const revoked = indexTxt.split("\n")
    .map((line) => line.split("\t"))
    .filter((f) => f[0] === "R")
    .map((f) => /\/CN=(smoke-[a-z0-9-]+)$/.exec(f[5] ?? "")?.[1])
    .filter((id): id is string => id !== undefined);
  return revoked.at(-1) ?? null;
}
