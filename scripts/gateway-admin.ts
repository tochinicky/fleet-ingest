/**
 * Enrol or revoke a gateway on the running Compose stack (D6, S8-5). The runbook's "Revoke a gateway" is this script.
 *
 *   node scripts/gateway-admin.ts enrol <gateway-id>    issue its certificate and register it with dynsec
 *   node scripts/gateway-admin.ts revoke <gateway-id>   revoke, reload the CRL, confirm the refusal, kick
 *   node scripts/gateway-admin.ts renew-crl             renew the CRL before it expires (every <30 days, S8-7)
 *
 * DEPLOY_HOST=fleet@<fqdn> runs the same against the deployed VM (stackAdmin.ts).
 *
 * Runs on the operator's machine, which holds the CA key (deploy/certs) and the dynsec admin's key
 * (deploy/admin/certs, mounted into no container). It fails loudly at the first step that doesn't do what it should,
 * and never kicks before the broker has been seen to refuse the revoked certificate (see gatewayAdmin.ts). The
 * stack's settings are in stackAdmin.ts.
 */
import { existsSync } from "node:fs";
import { join } from "node:path";
import { enrolGateway, revokeGateway } from "./gatewayAdmin.ts";
import { renewStackCrl, STACK_ADMIN, STACK_CERTS, stackCompose, stackEnrolment, stackRevocation } from "./stackAdmin.ts";

async function main(): Promise<void> {
  const [action, cn] = process.argv.slice(2);
  if (action === "renew-crl") {
    process.stdout.write(`CRL renewed and loaded; the broker accepts valid certificates. ${await renewStackCrl()}\n`);
    return;
  }
  if (!cn || (action !== "enrol" && action !== "revoke")) throw new Error("usage: node scripts/gateway-admin.ts enrol|revoke <gateway-id> | renew-crl");
  if (!existsSync(STACK_ADMIN.certFile)) throw new Error(`no dynsec admin certificate at ${STACK_ADMIN.certFile}: run scripts/dev-certs.sh`);
  if (action === "enrol") {
    await enrolGateway(stackEnrolment, cn);
    process.stdout.write(`enrolled ${cn}: certificate in deploy/certs, dynsec client registered (no roles)\n`);
    return;
  }
  if (!existsSync(join(STACK_CERTS, `${cn}.crt`))) throw new Error(`no certificate for ${cn} in deploy/certs: nothing to revoke`);
  const started = new Date();
  const times = await revokeGateway(stackRevocation, cn);
  const kicked = stackCompose(["logs", "--no-log-prefix", "--since", started.toISOString(), "mosquitto"]).split("\n").filter((l) => l.includes(cn) && /administrative action/i.test(l));
  process.stdout.write([
    `revoked ${cn}: CRL reload confirmed after ${(times.crlConfirmedMs / 1000).toFixed(1)} s (${times.probes} probe(s)), kicked at ${(times.kickedMs / 1000).toFixed(1)} s`,
    kicked.length ? `the broker closed its open connection: ${kicked[0]?.trim()}` : "the broker logged no open connection to close: the gateway wasn't connected",
    "",
  ].join("\n"));
}

main().catch((error: unknown) => {
  process.stderr.write(`gateway-admin: ${error instanceof Error ? error.message : String(error)}\n`);
  process.exit(1);
});
