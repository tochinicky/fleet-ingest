/**
 * The revocation procedure's order (S8-5): the kick happens only after the broker has been seen to refuse the revoked
 * certificate, never before, and a revocation the broker didn't confirm fails without kicking.
 */
import { describe, expect, it } from "vitest";
import { enrolGateway, revokeGateway } from "./gatewayAdmin.ts";
import type { Handshake, RevocationSteps } from "./gatewayAdmin.ts";

/** Fake steps that record what happened, with handshakes answering from `answers` (then the last one forever). */
function steps(answers: Handshake[]): { steps: RevocationSteps; log: string[] } {
  const log: string[] = [];
  let i = 0;
  return {
    log,
    steps: {
      revokeCertificate: async (cn) => void log.push(`revoke ${cn}`),
      reloadCrl: async () => void log.push("reload"),
      handshake: async () => {
        const answer = answers[Math.min(i++, answers.length - 1)] as Handshake;
        log.push(`probe ${typeof answer === "string" ? answer : "refused"}`);
        return answer;
      },
      disableClient: async (cn) => void log.push(`kick ${cn}`),
    },
  };
}

describe("revokeGateway", () => {
  it("revokes, reloads, waits for the refusal, and only then kicks", async () => {
    const fake = steps(["connected", "connected", "revoked"]);
    const times = await revokeGateway(fake.steps, "gw-x", 5000, 1);
    expect(fake.log).toEqual(["revoke gw-x", "reload", "probe connected", "probe connected", "probe revoked", "kick gw-x"]);
    expect(times.probes).toBe(3);
  });

  it("fails without kicking when the broker keeps accepting the certificate", async () => {
    const fake = steps(["connected"]);
    await expect(revokeGateway(fake.steps, "gw-x", 50, 5)).rejects.toThrow(/didn't confirm the CRL reload.*still accepted.*Not kicking/);
    expect(fake.log).not.toContain("kick gw-x");
  });

  it("doesn't take a refusal for another reason (a broker that's down) as confirmation", async () => {
    const fake = steps([{ refused: "connect ECONNREFUSED" }]);
    await expect(revokeGateway(fake.steps, "gw-x", 50, 5)).rejects.toThrow(/another reason \(connect ECONNREFUSED\)/);
    expect(fake.log).not.toContain("kick gw-x");
  });
});

describe("enrolGateway", () => {
  it("issues the certificate and registers the dynsec client together", async () => {
    const log: string[] = [];
    await enrolGateway({ issueCertificate: async (cn) => void log.push(`issue ${cn}`), createClient: async (cn) => void log.push(`client ${cn}`) }, "gw-y");
    expect(log).toEqual(["issue gw-y", "client gw-y"]);
  });
});
