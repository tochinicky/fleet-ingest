/**
 * Integration tests for identity and authorisation at the broker (D6), against the repo's real
 * `deploy/mosquitto/mosquitto.conf` and `acl`, with certificates made by `scripts/pki.sh`:
 * - a gateway can publish only its own readings and status, and read only its own commands;
 * - ingest reads everyone's readings through the shared subscription;
 * - no certificate, a foreign CA, or a revoked certificate is refused at the TLS handshake;
 * - what a CRL update does: SIGHUP reloads it for new handshakes, but an open connection stays up (runbook).
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { connect } from "mqtt";
import type { IClientOptions, MqttClient } from "mqtt";
import { GenericContainer, Wait } from "testcontainers";
import type { StartedTestContainer } from "testcontainers";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { MOSQUITTO_IMAGE } from "./support/containers.ts";

const ROOT = join(import.meta.dirname, "..", "..", "..");
const PKI = join(ROOT, "scripts", "pki.sh");
let certs: string;
let foreign: string;
let broker: StartedTestContainer;
let url: string;

/** Runs the PKI script. */
function pki(...args: string[]): void {
  execFileSync(PKI, args, { stdio: "ignore" });
}

/** TLS client options for a certificate CN issued in `dir` (default: the lab CA). */
function tlsFor(cn: string, dir = certs): IClientOptions {
  return {
    ca: readFileSync(join(certs, "ca.crt")),
    cert: readFileSync(join(dir, `${cn}.crt`)),
    key: readFileSync(join(dir, `${cn}.key`)),
    rejectUnauthorized: true,
  };
}

/** Tries to connect; resolves with the client, or with the error message if the broker or TLS refused. */
async function tryConnect(clientId: string, options: IClientOptions): Promise<MqttClient | string> {
  const client = connect(url, { protocolVersion: 5, clientId, reconnectPeriod: 0, connectTimeout: 5000, ...options });
  return new Promise((resolve) => {
    client.once("connect", () => resolve(client));
    client.once("error", (error) => {
      client.end(true);
      resolve(error.message);
    });
    client.once("close", () => resolve("closed"));
  });
}

/** Connects as `cn`, failing the test if refused. */
async function connectAs(cn: string, clientId = cn, extra: IClientOptions = {}): Promise<MqttClient> {
  const result = await tryConnect(clientId, { ...tlsFor(cn), ...extra });
  if (typeof result === "string") throw new Error(`${cn} refused: ${result}`);
  return result;
}

/** Collects messages a client receives. */
function collect(client: MqttClient): Array<{ topic: string; payload: string }> {
  const received: Array<{ topic: string; payload: string }> = [];
  client.on("message", (topic, payload) => received.push({ topic, payload: payload.toString() }));
  return received;
}

const pause = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

beforeAll(async () => {
  certs = mkdtempSync(join(tmpdir(), "fleet-pki-"));
  foreign = mkdtempSync(join(tmpdir(), "fleet-pki-foreign-"));
  pki("init", certs);
  pki("issue", certs, "ingest-1", "ops", "gw-a", "gw-b", "gw-revoked", "gw-later");
  pki("revoke", certs, "gw-revoked");
  pki("init", foreign);
  pki("issue", foreign, "gw-a");
  broker = await new GenericContainer(MOSQUITTO_IMAGE)
    .withCopyFilesToContainer([
      { source: join(ROOT, "deploy", "mosquitto", "mosquitto.conf"), target: "/mosquitto/config/mosquitto.conf" },
      { source: join(ROOT, "deploy", "mosquitto", "acl"), target: "/mosquitto/config/acl" },
    ])
    .withCopyDirectoriesToContainer([{ source: certs, target: "/mosquitto/certs" }])
    .withExposedPorts(8883)
    .withWaitStrategy(Wait.forLogMessage(/running/))
    .start();
  url = `mqtts://localhost:${broker.getMappedPort(8883)}`;
});

afterAll(async () => {
  await broker?.stop();
});

describe("authorisation (ACL by certificate CN)", () => {
  it("lets a gateway publish its own readings, and ingest receives them through the shared subscription", async () => {
    const ingest = await connectAs("ingest-1");
    const received = collect(ingest);
    await ingest.subscribeAsync("$share/ingest/homes/+/devices/+/telemetry", { qos: 1 });
    const gateway = await connectAs("gw-a");
    await gateway.publishAsync("homes/gw-a/devices/d1/telemetry", "own", { qos: 1 });
    await pause(500);
    expect(received.map((m) => m.payload)).toEqual(["own"]);
    await Promise.all([gateway.endAsync(), ingest.endAsync()]);
  });

  it("refuses a gateway publishing as another gateway, and nothing reaches ingest", async () => {
    const ingest = await connectAs("ingest-1", "ingest-1-b");
    const received = collect(ingest);
    await ingest.subscribeAsync("homes/+/devices/+/telemetry", { qos: 1 });
    const gateway = await connectAs("gw-a");
    const outcome = await gateway.publishAsync("homes/gw-b/devices/d1/telemetry", "impersonated", { qos: 1 }).then(() => "accepted", (e: Error) => e.message);
    await pause(500);
    expect(outcome).toMatch(/not authori[sz]ed|135/i);
    expect(received).toEqual([]);
    await Promise.all([gateway.endAsync(), ingest.endAsync()]);
  });

  it("refuses a gateway overwriting its own retained command", async () => {
    const gateway = await connectAs("gw-a");
    const command = await gateway.publishAsync("homes/gw-a/commands", '{"mode":"max"}', { qos: 1, retain: true }).then(() => "accepted", (e: Error) => e.message);
    expect(command).toMatch(/not authori[sz]ed|135/i);
    await gateway.endAsync();
  });

  it("delivers nothing a gateway may not read: other gateways' readings and $SYS (Mosquitto grants the SUBACK, then filters)", async () => {
    const spy = await connectAs("gw-a", "gw-a-spy");
    const received = collect(spy);
    await spy.subscribeAsync(["homes/gw-b/devices/+/telemetry", "$SYS/#", "homes/gw-a/commands"], { qos: 1 });
    const other = await connectAs("gw-b");
    await other.publishAsync("homes/gw-b/devices/d9/telemetry", "private", { qos: 1 });
    await pause(1500);
    expect(received).toEqual([]);
    await Promise.all([spy.endAsync(), other.endAsync()]);
  });

  it("lets ops read $SYS and nothing else", async () => {
    const ops = await connectAs("ops");
    const received = collect(ops);
    await ops.subscribeAsync(["$SYS/broker/version", "homes/+/status"], { qos: 0 });
    const gateway = await connectAs("gw-a", "gw-a-status");
    await gateway.publishAsync("homes/gw-a/status", '{"state":"online"}', { qos: 1 });
    await pause(1000);
    expect(received.map((m) => m.topic)).toEqual(["$SYS/broker/version"]);
    await Promise.all([ops.endAsync(), gateway.endAsync()]);
  });
});

describe("identity (TLS handshake)", () => {
  it("refuses a client without a certificate", async () => {
    const result = await tryConnect("anon", { ca: readFileSync(join(certs, "ca.crt")), rejectUnauthorized: true });
    expect(typeof result).toBe("string");
  });

  it("refuses a certificate from another CA, even with a valid gateway CN", async () => {
    const result = await tryConnect("gw-a-forged", { ...tlsFor("gw-a", foreign), ca: readFileSync(join(certs, "ca.crt")) });
    expect(typeof result).toBe("string");
  });

  it("refuses a revoked certificate", async () => {
    expect(typeof (await tryConnect("gw-revoked", tlsFor("gw-revoked")))).toBe("string");
  });

  it("refuses a newly revoked certificate after SIGHUP reloads the CRL, but leaves the gateway's open connection up", async () => {
    const before = await connectAs("gw-later");
    pki("revoke", certs, "gw-later");
    await broker.copyFilesToContainer([{ source: join(certs, "crl.pem"), target: "/mosquitto/certs/crl.pem" }]);
    await broker.exec(["kill", "-HUP", "1"]);
    await pause(1000);
    const afterHup = await tryConnect("gw-later-2", tlsFor("gw-later"));
    const stillPublishing = await before.publishAsync("homes/gw-later/devices/d1/telemetry", "after revocation", { qos: 1 }).then(() => true, () => false);
    if (typeof afterHup !== "string") await afterHup.endAsync();
    await before.endAsync().catch(() => undefined);
    expect({ newHandshakeRefused: typeof afterHup === "string", openConnectionStillPublishes: stillPublishing })
      .toEqual({ newHandshakeRefused: true, openConnectionStillPublishes: true });
  });
});
