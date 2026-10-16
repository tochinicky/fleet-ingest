/**
 * The gateway-admin steps on the running Compose stack (S8-5), shared by `scripts/gateway-admin.ts` and scenario 6.
 * The CA key is in deploy/certs; the dynsec admin's key is in deploy/admin/certs, which no container mounts.
 *
 * Two targets:
 * - **the local stack** (the lab): Compose here. Env COMPOSE_FILES (default deploy/compose.yaml; colon-separated for
 *   overlays), BROKER_URL (default mqtts://127.0.0.1:8883);
 * - **the deployed VM** (S8-6), with `DEPLOY_HOST=fleet@<fqdn>`: Compose runs there over SSH with the production
 *   profile, the new CRL is written into the VM's copy in place, and the broker is reached at <fqdn>:8883 with the
 *   TLS server name `mosquitto` (its certificate's name). The CA key never leaves this machine; only the CRL goes.
 *   "In place" matters: the broker mounts crl.pem as a single file, so a replaced file (a new inode, as rsync or
 *   scp would make) would go unseen until the container is recreated, and the reload would confirm nothing.
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { dynsecCommand, tryHandshake } from "./gatewayAdmin.ts";
import type { BrokerEndpoint, EnrolmentSteps, RevocationSteps } from "./gatewayAdmin.ts";

const ROOT = join(import.meta.dirname, "..");
export const STACK_CERTS = join(ROOT, "deploy", "certs");
export const STACK_ADMIN = { certFile: join(ROOT, "deploy", "admin", "certs", "dynsec-admin.crt"), keyFile: join(ROOT, "deploy", "admin", "certs", "dynsec-admin.key") };
const deployHost = process.env.DEPLOY_HOST;
/** The deployed VM's directory and Compose files (scripts/deploy.sh puts them there). */
const REMOTE_DIR = "/opt/fleet-ingest";
const REMOTE_COMPOSE = "docker compose --env-file deploy/production.env -f deploy/compose.yaml -f deploy/compose.production.yaml";

export const stackEndpoint: BrokerEndpoint = deployHost
  ? { url: process.env.BROKER_URL ?? `mqtts://${deployHost.replace(/^.*@/, "")}:8883`, caFile: join(STACK_CERTS, "ca.crt"), servername: process.env.BROKER_SERVERNAME ?? "mosquitto" }
  : { url: process.env.BROKER_URL ?? "mqtts://127.0.0.1:8883", caFile: join(STACK_CERTS, "ca.crt") };
const composeFiles = (process.env.COMPOSE_FILES ?? join(ROOT, "deploy", "compose.yaml")).split(":").flatMap((f) => ["-f", f]);

/** Runs `docker compose` on the target (here, or the deployed VM over SSH); throws with its stderr on failure. */
export function stackCompose(args: string[]): string {
  if (deployHost) {
    const quoted = args.map((a) => `'${a.replaceAll("'", "'\\''")}'`).join(" ");
    return execFileSync("ssh", [deployHost, `cd ${REMOTE_DIR} && ${REMOTE_COMPOSE} ${quoted}`], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
  }
  return execFileSync("docker", ["compose", ...composeFiles, ...args], { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
}

/** On the deployed VM, writes the new CRL into its existing file, keeping the inode the broker's mount points at. */
function pushCrl(): void {
  if (!deployHost) return;
  execFileSync("ssh", [deployHost, `cat > ${REMOTE_DIR}/deploy/certs/crl.pem`], { input: readFileSync(join(STACK_CERTS, "crl.pem")), stdio: ["pipe", "ignore", "pipe"] });
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
  reloadCrl: async () => {
    pushCrl();
    stackCompose(["kill", "-s", "HUP", "mosquitto"]);
  },
  handshake: (cn) => tryHandshake(stackEndpoint, join(STACK_CERTS, `${cn}.crt`), join(STACK_CERTS, `${cn}.key`), `${cn}-revocation-probe`),
  // A gateway enrolled before S8-5 has no dynsec client yet: register it, then disable it.
  disableClient: async (cn) => {
    await createClient(cn);
    await dynsecCommand(stackEndpoint, STACK_ADMIN, { command: "disableClient", username: cn });
  },
};
