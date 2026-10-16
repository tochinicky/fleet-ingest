/**
 * Reading a CRL's validity (S8-7): against a CRL made by the repo's own pki.sh and the times openssl reports for it.
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";
import { crlTimes } from "../src/core/crl.ts";

const ROOT = join(import.meta.dirname, "..", "..", "..");
const dir = mkdtempSync(join(tmpdir(), "crl-test-"));
afterAll(() => rmSync(dir, { recursive: true, force: true }));

/** openssl's reading of a CRL date field, e.g. "nextUpdate=Nov 14 21:29:00 2026 GMT". */
function opensslTime(field: "lastupdate" | "nextupdate"): Date {
  const out = execFileSync("openssl", ["crl", "-in", join(dir, "crl.pem"), "-noout", `-${field}`], { encoding: "utf8" });
  return new Date(out.split("=")[1]?.trim() ?? "");
}

describe("crlTimes", () => {
  it("reads thisUpdate and nextUpdate exactly as openssl does, from a CRL pki.sh made", () => {
    execFileSync(join(ROOT, "scripts", "pki.sh"), ["init", dir], { stdio: "ignore" });
    const times = crlTimes(readFileSync(join(dir, "crl.pem"), "utf8"));
    expect(times.thisUpdate).toEqual(opensslTime("lastupdate"));
    expect(times.nextUpdate).toEqual(opensslTime("nextupdate"));
    expect((times.nextUpdate?.getTime() ?? 0) - times.thisUpdate.getTime()).toBe(30 * 86_400_000);
  });

  it("refuses a file that isn't a CRL", () => {
    expect(() => crlTimes("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----")).toThrow(/no X509 CRL/);
  });
});
