/**
 * The gateway-admin steps on the running Compose stack (S8-5), shared by `scripts/gateway-admin.ts` and scenario 6.
 * The CA key is in deploy/certs; the dynsec admin's key is in deploy/admin/certs, which no container mounts.
 * Env: COMPOSE_FILES (default deploy/compose.yaml; colon-separated for overlays), BROKER_URL (default
 * mqtts://127.0.0.1:8883).
 */
import { execFileSync } from "node:child_process";
import { join } from "node:path";
import { dynsecCommand, tryHandshake } from "./gatewayAdmin.ts";
import type { BrokerEndpoint, EnrolmentSteps, RevocationSteps } from "./gatewayAdmin.ts";

const ROOT = join(import.meta.dirname, "..");
export const STACK_CERTS = join(ROOT, "deploy", "certs");
export const STACK_ADMIN = { certFile: join(ROOT, "deploy", "admin", "certs", "dynsec-admin.crt"), keyFile: join(ROOT, "deploy", "admin", "certs", "dynsec-admin.key") };
export const stackEndpoint: BrokerEndpoint = { url: process.env.BROKER_URL ?? "mqtts://127.0.0.1:8883", caFile: join(STACK_CERTS, "ca.crt") };
const composeFiles = (process.env.COMPOSE_FILES ?? join(ROOT, "deploy", "compose.yaml")).split(":").flatMap((f) => ["-f", f]);

/** Runs `docker compose` with the configured files; throws with its stderr on failure. */
export function stackCompose(args: string[]): string {
  return execFileSync("docker", ["compose", ...composeFiles, ...args], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
}

/** Runs `scripts/pki.sh` on the stack's CA. */
function pki(...args: string[]): void {
  execFileSync(join(ROOT, "scripts", "pki.sh"), args, { stdio: "ignore" });
}

/** Registers `cn` with dynsec; an existing client is fine (enrolment may be re-run). */
function createClient(cn: string): Promise<void> {
  return dynsecCommand(stackEndpoint, STACK_ADMIN, { command: "createClient", username: cn }, [/already exists/i]).then(() => undefined);
}

/** Enrolment on the stack: `pki.sh issue`, then a dynsec client with no roles. */
export const stackEnrolment: EnrolmentSteps = {
  issueCertificate: async (cn) => pki("issue", STACK_CERTS, cn),
  createClient,
};

/** Revocation on the stack: `pki.sh revoke`, SIGHUP, a handshake with the gateway's own certificate, `disableClient`. */
export const stackRevocation: RevocationSteps = {
  revokeCertificate: async (cn) => pki("revoke", STACK_CERTS, cn),
  reloadCrl: async () => void stackCompose(["kill", "-s", "HUP", "mosquitto"]),
  handshake: (cn) => tryHandshake(stackEndpoint, join(STACK_CERTS, `${cn}.crt`), join(STACK_CERTS, `${cn}.key`), `${cn}-revocation-probe`),
  // A gateway enrolled before S8-5 has no dynsec client yet: register it, then disable it.
  disableClient: async (cn) => {
    await createClient(cn);
    await dynsecCommand(stackEndpoint, STACK_ADMIN, { command: "disableClient", username: cn });
  },
};
