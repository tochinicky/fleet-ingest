/**
 * The post-deploy smoke check for the always-on VM (S8-6). From outside, the way a gateway and a browser reach it:
 *
 *   PUBLIC_HOST=<fqdn> API_TOKEN=... node scripts/smoke.ts     (Node 24: `nvm use` and `corepack pnpm install` at the repo root first)
 *
 * 1. HTTPS with a publicly trusted certificate (Node's default trust store, no -k): `/health/ready` answers ready.
 * 2. The API refuses `/fleet` without the token and serves it with it.
 * 3. Grafana answers under /grafana/ and refuses its API without a login (no anonymous viewer in production).
 * 4. MQTT on 8883: a client without a certificate is refused; this run's own gateway certificate connects (TLS
 *    server name `mosquitto`), and a reading it publishes is stored and served by the API within 30 s; and the most
 *    recently revoked smoke certificate, if there is one, is refused as revoked.
 * 5. Nothing else is reachable: Postgres, Prometheus, Grafana's and the API's own ports are closed from outside.
 * 6. The CRL (this machine's copy, which deploy.sh and gateway-admin push) is valid for at least 7 more days (S8-7).
 *
 * Each run issues its own gateway, `smoke-<UTC timestamp>` (smokeIdentity.ts), on this machine (scripts/pki.sh), and
 * registers it with dynsec when the admin certificate is here, so no run depends on an earlier one's certificate.
 * Revoke it afterwards to test revocation on the VM: `DEPLOY_HOST=fleet@<fqdn> node scripts/gateway-admin.ts revoke
 * <id>`; the next run then checks that it's refused.
 */
import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { connect as tcpConnect } from "node:net";
import { join } from "node:path";
import { connectAsync } from "mqtt";
import { crlTimes } from "../packages/broker-exporter/src/core/crl.ts";
import { dynsecCommand, tryHandshake } from "./gatewayAdmin.ts";
import { lastRevokedSmokeId, smokeGatewayId } from "./smokeIdentity.ts";

const ROOT = join(import.meta.dirname, "..");
const CERTS = join(ROOT, "deploy", "certs");
const GATEWAY = smokeGatewayId(new Date());
const host = process.env.PUBLIC_HOST ?? "";
const token = process.env.API_TOKEN ?? "";
const results: Array<{ check: string; pass: boolean; detail: string }> = [];

function record(check: string, pass: boolean, detail: string): void {
  results.push({ check, pass, detail });
  process.stdout.write(`${pass ? "PASS" : "FAIL"}  ${check}  (${detail})\n`);
}

async function status(path: string, auth = false): Promise<number> {
  const response = await fetch(`https://${host}${path}`, auth ? { headers: { authorization: `Bearer ${token}` } } : {});
  await response.arrayBuffer();
  return response.status;
}

/** True if a TCP connection to `port` succeeds within 3 s. */
function reachable(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const socket = tcpConnect({ host, port, timeout: 3000 });
    socket.once("connect", () => { socket.destroy(); resolve(true); });
    socket.once("timeout", () => { socket.destroy(); resolve(false); });
    socket.once("error", () => resolve(false));
  });
}

async function main(): Promise<void> {
  if (!host || token.length < 16) throw new Error("usage: PUBLIC_HOST=<fqdn> API_TOKEN=<the deployed token> node scripts/smoke.ts");
  const ready = await fetch(`https://${host}/health/ready`).then(async (r) => ({ code: r.status, body: await r.text() }), (e: Error) => ({ code: 0, body: `${e.message} ${(e.cause as Error | undefined)?.message ?? ""}` }));
  record("HTTPS with a trusted certificate; /health/ready is ready", ready.code === 200, `${ready.code} ${ready.body.slice(0, 120)}`);
  const [without, withToken] = [await status("/fleet"), await status("/fleet", true)];
  record("the API needs its token", without === 401 && withToken === 200, `without ${without}, with ${withToken}`);
  const [login, apiNoAuth] = [await status("/grafana/login"), await status("/grafana/api/dashboards/home")];
  record("Grafana is served under /grafana/ and its API needs a login", login === 200 && apiNoAuth === 401, `login page ${login}, API without login ${apiNoAuth}`);

  const endpoint = { url: `mqtts://${host}:8883`, caFile: join(CERTS, "ca.crt"), servername: "mosquitto" };
  const anonymous = await connectAsync(endpoint.url, { protocolVersion: 5, reconnectPeriod: 0, connectTimeout: 5000, ca: readFileSync(endpoint.caFile), servername: "mosquitto", clientId: "smoke-anonymous" })
    .then(async (c) => { await c.endAsync(); return "connected"; }, (e: Error) => e.message);
  record("MQTT: a client without a certificate is refused", anonymous !== "connected", anonymous.slice(0, 120));
  const revokedEarlier = lastRevokedSmokeId(readFileSync(join(CERTS, "index.txt"), "utf8"));
  if (revokedEarlier && existsSync(join(CERTS, `${revokedEarlier}.crt`))) {
    const refused = await tryHandshake(endpoint, join(CERTS, `${revokedEarlier}.crt`), join(CERTS, `${revokedEarlier}.key`), `${revokedEarlier}-probe`);
    record("MQTT: a revoked certificate is refused", refused === "revoked", `${revokedEarlier}: ${typeof refused === "string" ? refused : refused.refused.slice(0, 100)}`);
  } else {
    process.stdout.write("SKIP  MQTT: a revoked certificate is refused  (no revoked smoke certificate yet: revoke this run's to test it next time)\n");
  }
  execFileSync(join(ROOT, "scripts", "pki.sh"), ["issue", CERTS, GATEWAY], { stdio: "ignore" });
  const admin = { certFile: join(ROOT, "deploy", "admin", "certs", "dynsec-admin.crt"), keyFile: join(ROOT, "deploy", "admin", "certs", "dynsec-admin.key") };
  if (existsSync(admin.certFile)) await dynsecCommand(endpoint, admin, { command: "createClient", username: GATEWAY }, [/already exists/i]);
  const handshake = await tryHandshake(endpoint, join(CERTS, `${GATEWAY}.crt`), join(CERTS, `${GATEWAY}.key`), `${GATEWAY}-probe`);
  record(`MQTT: this run's gateway certificate (${GATEWAY}) connects`, handshake === "connected", typeof handshake === "string" ? handshake : handshake.refused.slice(0, 120));
  if (handshake === "connected") {
    const deviceId = `${GATEWAY}-bat-1`;
    const seq = Math.floor(Date.now() / 1000);
    const client = await connectAsync(endpoint.url, {
      protocolVersion: 5, clientId: GATEWAY, reconnectPeriod: 0, connectTimeout: 5000, servername: "mosquitto",
      ca: readFileSync(endpoint.caFile), cert: readFileSync(join(CERTS, `${GATEWAY}.crt`)), key: readFileSync(join(CERTS, `${GATEWAY}.key`)),
    });
    await client.publishAsync(`homes/${GATEWAY}/devices/${deviceId}/telemetry`, JSON.stringify({ deviceId, epoch: 1, seq, timestamp: new Date().toISOString(), kind: "Battery", socPercent: 50 }), { qos: 1 });
    await client.endAsync();
    let stored = 0;
    for (let i = 0; i < 30 && stored !== seq; i++) {
      const r = await fetch(`https://${host}/devices/${deviceId}/state`, { headers: { authorization: `Bearer ${token}` } });
      stored = r.ok ? Number(((await r.json()) as { seq?: number }).seq) : 0;
      if (stored !== seq) await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    record("MQTT: a reading published over 8883 is stored and served by the API", stored === seq, `device ${deviceId}: state seq ${stored}, sent ${seq}`);
  }
  const open = [];
  for (const port of [5432, 9090, 9093, 3000, 8080, 9464]) if (await reachable(port)) open.push(port);
  record("nothing else is reachable from outside (5432, 9090, 9093, 3000, 8080, 9464)", open.length === 0, open.length ? `open: ${open.join(", ")}` : "all closed");
  const { nextUpdate } = crlTimes(readFileSync(join(CERTS, "crl.pem"), "utf8"));
  const daysLeft = nextUpdate ? (nextUpdate.getTime() - Date.now()) / 86_400_000 : 0;
  record("the CRL is valid for at least 7 more days (renew: gateway-admin.ts renew-crl)", daysLeft >= 7, `nextUpdate ${nextUpdate?.toISOString() ?? "none"}, ${daysLeft.toFixed(1)} days left`);
  const failed = results.filter((r) => !r.pass).length;
  process.stdout.write(`smoke: ${failed ? `FAIL (${failed} of ${results.length})` : "PASS"}; this run's gateway: ${GATEWAY}\n`);
  process.exit(failed ? 1 : 0);
}

main().catch((error: unknown) => {
  process.stdout.write(`FAIL  the smoke check could not complete  (${error instanceof Error ? error.message : String(error)})\n`);
  process.exit(1);
});
