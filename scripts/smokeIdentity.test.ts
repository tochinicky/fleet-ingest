/** The smoke check's identities (S8-8): a new one per run, and the last revoked one for the refusal check. */
import { describe, expect, it } from "vitest";
import { lastRevokedSmokeId, smokeGatewayId } from "./smokeIdentity.ts";

describe("smoke identities", () => {
  it("is new for every run: the UTC time to the second, in a valid lowercase CN", () => {
    const a = smokeGatewayId(new Date("2026-10-16T13:05:07.123Z"));
    expect(a).toBe("smoke-20261016t130507z");
    expect(smokeGatewayId(new Date("2026-10-16T13:05:08Z"))).not.toBe(a);
    expect(a).toMatch(/^[a-z0-9-]+$/);
  });

  it("finds the most recently revoked smoke identity in the CA's database, and ignores valid and non-smoke ones", () => {
    const index = [
      "V\t361016000000Z\t\t1001\tunknown\t/CN=smoke-gw-1",
      "R\t361016000000Z\t261016120000Z\t1002\tunknown\t/CN=smoke-gw-1",
      "R\t361016000000Z\t261016120500Z\t1003\tunknown\t/CN=gw-stolen",
      "R\t361016000000Z\t261016121000Z\t1004\tunknown\t/CN=smoke-20261016t121000z",
      "V\t361016000000Z\t\t1005\tunknown\t/CN=smoke-20261016t130000z",
    ].join("\n");
    expect(lastRevokedSmokeId(index)).toBe("smoke-20261016t121000z");
    expect(lastRevokedSmokeId("V\t361016000000Z\t\t1001\tunknown\t/CN=smoke-gw-1\n")).toBeNull();
  });
});
