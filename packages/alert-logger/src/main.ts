/**
 * The alert receiver for the lab (a webhook that logs; no real paging). Alertmanager posts its webhook
 * payloads to `POST /alerts`. Each alert is logged as one JSON line and kept in memory, so `GET /alerts` lets the demo
 * scripts check what fired and when it resolved.
 */
import { createServer } from "node:http";
import type { IncomingMessage, ServerResponse } from "node:http";
import { pino } from "pino";
import { z } from "zod";

/** One alert as Alertmanager sends it (only the fields kept here). */
const alertSchema = z.object({
  status: z.enum(["firing", "resolved"]),
  labels: z.record(z.string(), z.string()),
  annotations: z.record(z.string(), z.string()).default({}),
  startsAt: z.string(),
  endsAt: z.string().optional(),
});

/** Alertmanager's webhook body. */
const webhookSchema = z.object({ alerts: z.array(alertSchema) });

/** A received alert with the time it arrived here. */
type ReceivedAlert = z.infer<typeof alertSchema> & { receivedAt: string };

/** How many alerts are kept for `GET /alerts`; older ones are forgotten. */
const KEEP = 2000;

/** Reads a request body as text. */
async function readBody(request: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) chunks.push(chunk as Buffer);
  return Buffer.concat(chunks).toString("utf8");
}

/** Writes a JSON response. */
function sendJson(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { "content-type": "application/json" }).end(JSON.stringify(body));
}

/** Starts the receiver on PORT (default 9095). */
function main(): void {
  const port = z.coerce.number().int().default(9095).parse(process.env.PORT);
  const log = pino({ base: { service: "alert-logger" } });
  const received: ReceivedAlert[] = [];
  createServer((request, response) => {
    if (request.method === "GET" && request.url === "/alerts") return sendJson(response, 200, { alerts: received });
    if (request.method !== "POST" || request.url !== "/alerts") return sendJson(response, 404, { error: "not found" });
    void readBody(request).then((text) => {
      const parsed = webhookSchema.safeParse(JSON.parse(text || "{}"));
      if (!parsed.success) return sendJson(response, 400, { error: "not an Alertmanager webhook" });
      for (const alert of parsed.data.alerts) {
        const entry = { ...alert, receivedAt: new Date().toISOString() };
        received.push(entry);
        log.info({ alert: alert.labels.alertname, status: alert.status, labels: alert.labels, summary: alert.annotations.summary }, "alert");
      }
      received.splice(0, Math.max(0, received.length - KEEP));
      return sendJson(response, 200, { ok: true });
    }).catch(() => sendJson(response, 400, { error: "unreadable body" }));
  }).listen(port);
  log.info({ port }, "alert logger running");
}

main();
