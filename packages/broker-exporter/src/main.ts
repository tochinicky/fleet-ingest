/**
 * Composition root of the broker exporter: it turns what Mosquitto says about itself into Prometheus metrics.
 * - `$SYS`: subscribes with the `ops` certificate (the ACL lets it read `$SYS/#` and nothing else) and exports each
 *   value in `SYS_METRICS` as a gauge.
 * - Log-watch: follows the broker's log file and counts drop episodes and session expiries per client (S6-3).
 *   This is the "log-watch sidecar" of D3, folded into this one service.
 * - The CRL's validity (S8-7): reads the CRL file the broker loads, every minute, and exports when it was issued and
 *   when it expires. An unreadable or unparseable CRL exports an expiry of 0, so CrlExpiringSoon fires on it too:
 *   a CRL nobody can check is treated as one about to lock every gateway out.
 */
import { readFileSync } from "node:fs";
import { connect } from "mqtt";
import { pino } from "pino";
import { z } from "zod";
import { Counter, createRegistry, Gauge, startMetricsServer } from "@fleet-ingest/observability";
import { followLog } from "./adapters/logFollower.ts";
import { classifyLogLine } from "./core/logEvents.ts";
import { crlTimes } from "./core/crl.ts";
import { parseSysValue, SYS_METRICS } from "./core/sysTopics.ts";

/** Every setting the exporter reads. */
const configSchema = z.object({
  MQTT_URL: z.url({ protocol: /^mqtts?$/ }),
  MQTT_CA_FILE: z.string(),
  MQTT_CERT_FILE: z.string(),
  MQTT_KEY_FILE: z.string(),
  BROKER_LOG_FILE: z.string().default("/mosquitto/log/mosquitto.log"),
  /** The CRL the broker loads (mounted read-only, public data). Unset: no CRL metrics. */
  CRL_FILE: z.string().optional(),
  METRICS_PORT: z.coerce.number().int().min(1).max(65535).default(9464),
  LOG_LEVEL: z.enum(["fatal", "error", "warn", "info", "debug"]).default("info"),
});

/** Starts the exporter. */
function main(): void {
  const parsed = configSchema.safeParse(process.env);
  if (!parsed.success) throw new Error(`invalid configuration:\n${parsed.error.issues.map((i) => `  ${i.path.join(".")}: ${i.message}`).join("\n")}`);
  const config = parsed.data;
  const log = pino({ level: config.LOG_LEVEL, base: { service: "broker-exporter" } });
  const registry = createRegistry("broker-exporter");
  const registers = [registry];
  const gauges = new Map(SYS_METRICS.map((m) => [m.topic, new Gauge({ name: m.name, help: m.help, registers })]));
  const lastUpdate = new Gauge({ name: "broker_sys_last_update_timestamp_seconds", help: "When a $SYS value last arrived (a stale value means the exporter or the broker is stuck).", registers });
  const logEvents = new Counter({ name: "broker_log_events_total", help: "Broker log lines that matter: kind=dropping (queue bound hit) or expiring (session expired, its queue discarded), per client.", labelNames: ["kind", "client"], registers });
  const logEventsByKind = new Counter({ name: "broker_log_events_by_kind_total", help: "The same events per kind only; created at 0 so alerts can see the first one (S6-4).", labelNames: ["kind"], registers });
  for (const kind of ["dropping", "expiring"]) logEventsByKind.labels(kind).inc(0);

  const client = connect(config.MQTT_URL, {
    clientId: "broker-exporter", protocolVersion: 5, clean: true, reconnectPeriod: 2000, rejectUnauthorized: true,
    ca: readFileSync(config.MQTT_CA_FILE), cert: readFileSync(config.MQTT_CERT_FILE), key: readFileSync(config.MQTT_KEY_FILE),
  });
  client.on("connect", () => void client.subscribeAsync(SYS_METRICS.map((m) => m.topic), { qos: 0 }).catch((error) => log.warn({ err: error }, "subscribe failed")));
  client.on("error", (error) => log.warn({ err: error }, "broker connection problem"));
  client.on("message", (topic, payload) => {
    const value = parseSysValue(payload.toString());
    if (value === null) return;
    gauges.get(topic)?.set(value);
    lastUpdate.set(Date.now() / 1000);
  });

  const logReadErrors = new Counter({ name: "broker_log_read_errors_total", help: "Failed reads of the broker log file (permissions, rotation); log-watch is blind while this rises.", registers });
  followLog(config.BROKER_LOG_FILE, (line) => {
    const event = classifyLogLine(line);
    if (!event) return;
    logEvents.labels(event.kind, event.clientId).inc();
    logEventsByKind.labels(event.kind).inc();
  }, (error) => {
    logReadErrors.inc();
    log.warn({ err: error }, "reading the broker log failed; retrying");
  });
  if (config.CRL_FILE) {
    const crlFile = config.CRL_FILE;
    const crlNext = new Gauge({ name: "broker_crl_next_update_timestamp_seconds", help: "When the broker's CRL expires; after it, every client certificate is refused. 0 if the CRL can't be read (S8-7).", registers });
    const crlThis = new Gauge({ name: "broker_crl_this_update_timestamp_seconds", help: "When the broker's CRL was issued (its last renewal or revocation).", registers });
    const readCrl = (): void => {
      try {
        const times = crlTimes(readFileSync(crlFile, "utf8"));
        crlThis.set(times.thisUpdate.getTime() / 1000);
        crlNext.set((times.nextUpdate?.getTime() ?? 0) / 1000);
      } catch (error) {
        crlNext.set(0);
        log.warn({ err: error, crlFile }, "reading the CRL failed");
      }
    };
    readCrl();
    setInterval(readCrl, 60_000);
  }
  startMetricsServer(registry, config.METRICS_PORT);
  log.info({ topics: SYS_METRICS.length, logFile: config.BROKER_LOG_FILE }, "broker exporter running");
}

main();
