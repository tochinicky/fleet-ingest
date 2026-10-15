/**
 * Composition root of fleet-sim: validates config, starts N virtual gateways with M devices each, and has every
 * device record a reading each interval. Gateways are spread evenly across the interval, so the fleet sends a steady
 * stream rather than a burst every 5 s. Slice 1 has no chaos knobs; they arrive with the scenarios that need them.
 */
import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import { join } from "node:path";
import type { Server, ServerResponse } from "node:http";
import { pino } from "pino";
import type { Logger } from "pino";
import type { IncomingMessage } from "node:http";
import type { TelemetryMessage } from "@fleet-ingest/contract";
import { configSchema } from "./config.ts";
import { AckLedger } from "./core/ackLedger.ts";
import { decide, NO_CHAOS, parseChaosSettings, poisonMessage, Reorderer } from "./core/chaos.ts";
import type { ChaosSettings } from "./core/chaos.ts";
import { devicesFor, measure, seededRandom } from "./core/devices.ts";
import type { SimDevice } from "./core/devices.ts";
import { Sequencer } from "./core/sequencer.ts";
import { startVirtualGateway } from "./adapters/virtualGateway.ts";
import type { VirtualGateway } from "./adapters/virtualGateway.ts";


/** Gateway IDs like `sim-gw-0001`, padded so they sort in order. */
function gatewayIdFor(prefix: string, index: number): string {
  return `${prefix}-${String(index + 1).padStart(4, "0")}`;
}

/** Starts the fleet and runs until SIGTERM/SIGINT, when every gateway disconnects cleanly. */
async function main(): Promise<void> {
  const parsed = configSchema.safeParse(process.env);
  if (!parsed.success) throw new Error(`invalid configuration:\n${parsed.error.issues.map((i) => `  ${i.path.join(".")}: ${i.message}`).join("\n")}`);
  const config = parsed.data;
  if (config.MQTT_URL.startsWith("mqtts:") && !config.CERTS_DIR) throw new Error("invalid configuration:\n  mqtts:// needs CERTS_DIR");
  const log = pino({ level: config.LOG_LEVEL, base: { service: "fleet-sim" } });
  const random = seededRandom(config.SEED);
  const timers: NodeJS.Timeout[] = [];
  const gateways: VirtualGateway[] = [];
  const ledger = new AckLedger();
  const sequencers = new Map<string, Sequencer>();
  const chaos = { current: NO_CHAOS };
  const reorderers: Array<Reorderer<TelemetryMessage>> = [];

  for (let i = 0; i < config.GATEWAY_COUNT; i++) {
    const gatewayId = gatewayIdFor(config.GATEWAY_ID_PREFIX, i);
    const gateway = startVirtualGateway(
      gatewayId,
      {
        brokerUrl: config.MQTT_URL_CUTTABLE && (i + 1) % config.CUTTABLE_EVERY === 0 ? config.MQTT_URL_CUTTABLE : config.MQTT_URL,
        keepaliveSeconds: config.KEEPALIVE_SECONDS,
        connectTimeoutMs: config.CONNECT_TIMEOUT_MS,
        reconnectJitterMaxMs: config.RECONNECT_JITTER_MAX_MS,
        random: Math.random,
        ...(config.MQTT_SERVERNAME ? { serverName: config.MQTT_SERVERNAME } : {}),
        window: config.UPLINK_WINDOW, replayPerSecond: config.REPLAY_PER_SECOND,
        bufferCapacity: config.BUFFER_CAPACITY, statusIntervalMs: config.STATUS_INTERVAL_MS, mode: "surplus",
        ...(config.CERTS_DIR ? { tls: gatewayTls(config.CERTS_DIR, gatewayId) } : {}),
      },
      ledger,
      (error) => log.warn({ gatewayId, err: error }, "gateway connection problem"),
    );
    gateways.push(gateway);
    const sequencer = new Sequencer(config.EPOCH);
    sequencers.set(gatewayId, sequencer);
    const devices = devicesFor(gatewayId, config.DEVICES_PER_GATEWAY);
    const neighbour = devicesFor(gatewayIdFor(config.GATEWAY_ID_PREFIX, (i + 1) % config.GATEWAY_COUNT), config.DEVICES_PER_GATEWAY);
    const reorderer = new Reorderer<TelemetryMessage>(random);
    reorderers.push(reorderer);
    const gatewayChaos: GatewayChaos = {
      settings: () => chaos.current,
      reorderer,
      foreignDeviceId: (device) => neighbour[devices.indexOf(device)]?.deviceId ?? `${device.deviceId}-foreign`,
    };
    const offsetMs = Math.floor((i / config.GATEWAY_COUNT) * config.READING_INTERVAL_MS);
    scheduleReadings(timers, offsetMs, config.READING_INTERVAL_MS, () => recordReadings(gateway, devices, sequencer, random, new Date(), gatewayChaos));
  }
  log.info({ gateways: config.GATEWAY_COUNT, devicesPerGateway: config.DEVICES_PER_GATEWAY, epoch: config.EPOCH }, "fleet running");

  timers.push(setInterval(() => logUplink(log, gateways), 30_000));
  startControlApi(config.CONTROL_PORT, ledger, gateways, sequencers, {
    get: () => chaos.current,
    set: (next) => {
      chaos.current = next;
      reorderers.forEach((reorderer, i) => {
        const gateway = gateways[i];
        if (gateway && reorderer.size() >= Math.max(1, next.reorderWindow)) reorderer.drain().forEach((m) => gateway.record(m));
      });
      log.info({ chaos: next }, "chaos settings changed");
    },
  });
  process.on("SIGTERM", () => void shutDown(timers, gateways));
  process.on("SIGINT", () => void shutDown(timers, gateways));
}

/** Reads the CA and this gateway's own certificate and key; a missing file fails startup with its path. */
function gatewayTls(certsDir: string, gatewayId: string): { ca: Buffer; cert: Buffer; key: Buffer } {
  return {
    ca: readFileSync(join(certsDir, "ca.crt")),
    cert: readFileSync(join(certsDir, `${gatewayId}.crt`)),
    key: readFileSync(join(certsDir, `${gatewayId}.key`)),
  };
}

/** Records one reading per device into the gateway's outbox, all stamped `at`, numbered by the gateway's sequencer. */
function recordReadings(gateway: VirtualGateway, devices: SimDevice[], sequencer: Sequencer, random: () => number, at: Date, chaos: GatewayChaos): void {
  for (const device of devices) {
    const reading: TelemetryMessage = { deviceId: device.deviceId, ...sequencer.next(device.deviceId), timestamp: at.toISOString(), kind: device.kind, ...measure(device, at, random) };
    const decision = decide(chaos.settings(), random);
    for (let copy = 0; copy < decision.copies; copy++) {
      for (const released of chaos.reorderer.push(reading, chaos.settings().reorderWindow)) gateway.record(released);
    }
    if (decision.poison) gateway.publishRaw(poisonMessage(decision.poison, reading, chaos.foreignDeviceId(device), at));
  }
}

/** One gateway's chaos state: the shared settings, its own reorder window, and a neighbour's device for poison. */
interface GatewayChaos {
  settings: () => ChaosSettings;
  reorderer: Reorderer<TelemetryMessage>;
  foreignDeviceId: (device: SimDevice) => string;
}

/**
 * Runs `tick` once after `offsetMs`, then every `intervalMs`. The offset spreads gateways across the interval.
 * Every timer it creates goes into `timers`, so shutdown can clear them.
 */
function scheduleReadings(timers: NodeJS.Timeout[], offsetMs: number, intervalMs: number, tick: () => void): void {
  timers.push(setTimeout(() => {
    tick();
    timers.push(setInterval(tick, intervalMs));
  }, offsetMs));
}

/** Logs how many readings the whole fleet has waiting in its outboxes, and how many it has dropped. */
function logUplink(log: Logger, gateways: VirtualGateway[]): void {
  const buffered = gateways.reduce((sum, g) => sum + g.buffered(), 0);
  const dropped = gateways.reduce((sum, g) => sum + g.dropped(), 0);
  log.info({ buffered, dropped }, "fleet uplink");
}

/**
 * Serves the control API for the scenario scripts:
 * - `GET /ledger`: every reading the broker acknowledged, as per-device seq ranges, and the acknowledged statuses;
 * - `GET /stats`: the fleet's outbox counters;
 * - `POST /chaos/power-loss?gateway=<id>&rollback=<n>`: that gateway loses power (next epoch, seq rolled back by n);
 * - `GET /chaos/settings`, `POST /chaos/settings {duplicateRate?, reorderWindow?, poisonRate?}`: the chaos knobs
 *   (scenarios 1 and 5). Lowering the reorder window releases what the reorderers held.
 * Network cuts aren't here: they're toxiproxy's job (scenario 2).
 */
function startControlApi(port: number, ledger: AckLedger, gateways: VirtualGateway[], sequencers: Map<string, Sequencer>, chaos: ChaosControl): Server {
  const server = createServer((request, response) => {
    const url = new URL(request.url ?? "/", "http://fleet-sim");
    if (request.method === "GET" && url.pathname === "/ledger") return sendJson(response, 200, { devices: ledger.entries(), statuses: ledger.statuses() });
    if (request.method === "GET" && url.pathname === "/stats") {
      return sendJson(response, 200, { gateways: gateways.length, buffered: gateways.reduce((s, g) => s + g.buffered(), 0), dropped: gateways.reduce((s, g) => s + g.dropped(), 0) });
    }
    if (request.method === "POST" && url.pathname === "/chaos/power-loss") return sendJson(response, ...powerLoss(sequencers, url.searchParams));
    if (request.method === "GET" && url.pathname === "/chaos/settings") return sendJson(response, 200, chaos.get());
    if (request.method === "POST" && url.pathname === "/chaos/settings") return void updateChaos(request, response, chaos);
    return sendJson(response, 404, { error: "not found" });
  });
  return server.listen(port);
}

/** Reading and changing the chaos settings, for the control API. */
interface ChaosControl {
  get: () => ChaosSettings;
  set: (next: ChaosSettings) => void;
}

/** Applies a chaos settings update from a request body; answers 400 with the reason if it's invalid. */
async function updateChaos(request: IncomingMessage, response: ServerResponse, chaos: ChaosControl): Promise<void> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(chunk as Buffer);
  let body: unknown;
  try {
    body = JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
  } catch {
    return sendJson(response, 400, { error: "body must be JSON" });
  }
  const next = parseChaosSettings(body, chaos.get());
  if (typeof next === "string") return sendJson(response, 400, { error: next });
  chaos.set(next);
  sendJson(response, 200, next);
}

/** Writes a JSON response with the given status. */
function sendJson(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}

/** Applies a power loss to one gateway's numbering; returns the HTTP status and body to answer with. */
function powerLoss(sequencers: Map<string, Sequencer>, params: URLSearchParams): [number, unknown] {
  const gatewayId = params.get("gateway") ?? "";
  const rollback = Number(params.get("rollback") ?? "5");
  const sequencer = sequencers.get(gatewayId);
  if (!sequencer || !Number.isInteger(rollback) || rollback < 0) return [400, { error: "need ?gateway=<known id>&rollback=<n ≥ 0>" }];
  sequencer.powerLoss(rollback);
  return [200, { gatewayId, epoch: sequencer.epoch(), rollback }];
}

/** Stops producing readings, disconnects every gateway cleanly (each publishes `offline` itself), and exits 0. */
async function shutDown(timers: NodeJS.Timeout[], gateways: VirtualGateway[]): Promise<void> {
  timers.forEach((timer) => clearTimeout(timer));
  await Promise.all(gateways.map((g) => g.stop()));
  process.exit(0);
}

await main();
