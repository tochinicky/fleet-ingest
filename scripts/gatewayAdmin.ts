/**
 * Enrolling and revoking a gateway (D6, S8-5). Revocation closes the gateway's open connection without a broker
 * restart, through the dynamic security plugin's `disableClient`, which kicks a connected client.
 *
 * The order is the guarantee. A kicked client reconnects at once, and with `use_identity_as_username` the broker
 * never consults "disabled" at connect: only the CRL refuses it. So the procedure kicks only after a handshake with
 * the revoked certificate has been seen to fail, which proves the broker reloaded the CRL. Kicking before that would
 * let the gateway straight back in on a still-valid certificate, and look like success. If the refusal isn't seen in
 * time, the procedure fails loudly and never kicks.
 *
 * Pure orchestration over injected steps, so the order is unit-tested; the adapters that talk to the broker are below.
 */
import { readFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";
import { connect, connectAsync } from "mqtt";

/** What a handshake with a gateway's certificate did. */
export type Handshake = "connected" | "revoked" | { refused: string };

/** The steps a revocation needs. */
export interface RevocationSteps {
  /** Revokes the certificate and regenerates the CRL (`pki.sh revoke`). */
  revokeCertificate(cn: string): Promise<void>;
  /** Tells the broker to reload its CRL (SIGHUP). */
  reloadCrl(): Promise<void>;
  /** Tries a TLS handshake and MQTT connect with the gateway's own certificate. */
  handshake(cn: string): Promise<Handshake>;
  /** Kicks the gateway's open connection (dynsec `disableClient`). */
  disableClient(cn: string): Promise<void>;
}

/** When each step of a revocation finished, in ms from its start. */
export interface RevocationTimes {
  crlConfirmedMs: number;
  kickedMs: number;
  probes: number;
}

/**
 * Revokes `cn`: certificate and CRL, reload, a confirmed refusal, then the kick. Throws, without kicking, if the
 * broker still accepts the certificate after `confirmTimeoutMs`, or refuses it for some reason other than revocation
 * (a broker that's down proves nothing about its CRL).
 */
export async function revokeGateway(steps: RevocationSteps, cn: string, confirmTimeoutMs = 10_000, pollMs = 250): Promise<RevocationTimes> {
  const started = Date.now();
  await steps.revokeCertificate(cn);
  await steps.reloadCrl();
  let probes = 0;
  for (;;) {
    probes++;
    const last = await steps.handshake(cn);
    if (last === "revoked") break;
    if (Date.now() - started > confirmTimeoutMs) {
      const seen = last === "connected" ? "still accepted" : `refused for another reason (${last.refused})`;
      throw new Error(`the broker didn't confirm the CRL reload for ${cn} within ${confirmTimeoutMs / 1000} s: its certificate is ${seen}. Not kicking: a kick now would let it straight back in. See the runbook.`);
    }
    await sleep(pollMs);
  }
  const crlConfirmedMs = Date.now() - started;
  await steps.disableClient(cn);
  return { crlConfirmedMs, kickedMs: Date.now() - started, probes };
}

/** The steps an enrolment needs. */
export interface EnrolmentSteps {
  /** Issues the gateway's certificate (`pki.sh issue`). */
  issueCertificate(cn: string): Promise<void>;
  /** Registers the gateway with the dynamic security plugin, with no roles, so it can be kicked later. */
  createClient(cn: string): Promise<void>;
}

/** Enrols `cn`: its certificate and its dynsec client, in one step, so no gateway exists that can't be kicked. */
export async function enrolGateway(steps: EnrolmentSteps, cn: string): Promise<void> {
  await steps.issueCertificate(cn);
  await steps.createClient(cn);
}

/** A broker endpoint and the TLS files to reach it with. */
export interface BrokerEndpoint {
  url: string;
  caFile: string;
  /** The SNI/verification name, when `url` is an address the broker certificate doesn't name. */
  servername?: string;
}

/** Tries one connect with a certificate; "revoked" only when the TLS error says so. */
export async function tryHandshake(endpoint: BrokerEndpoint, certFile: string, keyFile: string, clientId: string): Promise<Handshake> {
  const client = connect(endpoint.url, {
    protocolVersion: 5, clientId, reconnectPeriod: 0, connectTimeout: 5000, rejectUnauthorized: true,
    ca: readFileSync(endpoint.caFile), cert: readFileSync(certFile), key: readFileSync(keyFile),
    ...(endpoint.servername ? { servername: endpoint.servername } : {}),
  });
  return new Promise((resolve) => {
    client.once("connect", () => {
      client.end(true);
      resolve("connected");
    });
    client.once("error", (error) => {
      client.end(true);
      resolve(/revoked/i.test(error.message) ? "revoked" : { refused: error.message });
    });
    client.once("close", () => resolve({ refused: "connection closed" }));
  });
}

/** One dynsec command's answer. */
interface DynsecResponse {
  command: string;
  error?: string;
  data?: unknown;
}

/**
 * Sends dynsec commands as the admin client (CN `dynsec-admin`, whose certificate lives only with the operator, never
 * in a service container) and returns the responses, in order.
 */
export async function dynsec(endpoint: BrokerEndpoint, admin: { certFile: string; keyFile: string }, commands: Array<Record<string, unknown>>): Promise<DynsecResponse[]> {
  const client = await connectAsync(endpoint.url, {
    protocolVersion: 5, clientId: `dynsec-admin-${process.pid}-${Date.now()}`, reconnectPeriod: 0, connectTimeout: 5000, rejectUnauthorized: true,
    ca: readFileSync(endpoint.caFile), cert: readFileSync(admin.certFile), key: readFileSync(admin.keyFile),
    ...(endpoint.servername ? { servername: endpoint.servername } : {}),
  }).catch((error: Error) => {
    throw new Error(`the dynsec admin client couldn't connect: ${error.message}`);
  });
  try {
    const answer = new Promise<DynsecResponse[]>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("no answer from the dynamic security plugin within 5 s")), 5000);
      client.on("message", (_topic: string, payload: Buffer) => {
        clearTimeout(timer);
        resolve((JSON.parse(payload.toString()) as { responses: DynsecResponse[] }).responses);
      });
    });
    await client.subscribeAsync("$CONTROL/dynamic-security/v1/response", { qos: 1 });
    await client.publishAsync("$CONTROL/dynamic-security/v1", JSON.stringify({ commands }), { qos: 1 });
    return await answer;
  } finally {
    await client.endAsync();
  }
}

/** Runs one dynsec command and throws on its error, unless the error is in `tolerated`. */
export async function dynsecCommand(endpoint: BrokerEndpoint, admin: { certFile: string; keyFile: string }, command: Record<string, unknown>, tolerated: RegExp[] = []): Promise<unknown> {
  const [response] = await dynsec(endpoint, admin, [command]);
  if (!response) throw new Error(`dynsec ${String(command.command)}: no response`);
  if (response.error && !tolerated.some((t) => t.test(response.error as string))) throw new Error(`dynsec ${String(command.command)}: ${response.error}`);
  return response.data;
}
