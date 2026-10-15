/**
 * Shared observability plumbing: a Prometheus registry with Node's default process metrics, and a tiny HTTP server
 * that serves it on `/metrics`. Each service defines its own metrics on the registry this returns.
 */
import { createServer } from "node:http";
import type { Server } from "node:http";
import { collectDefaultMetrics, Registry } from "prom-client";

export { Counter, Gauge, Histogram, Registry } from "prom-client";

/** A registry with the default process metrics (RSS, CPU, event-loop lag, GC), labelled with the service name. */
export function createRegistry(service: string): Registry {
  const registry = new Registry();
  registry.setDefaultLabels({ service });
  collectDefaultMetrics({ register: registry });
  return registry;
}

/**
 * Serves `registry` on `GET /metrics` at `port`, on every interface (inside the Compose network only; Compose
 * doesn't publish these ports). Anything else is 404.
 */
export function startMetricsServer(registry: Registry, port: number): Server {
  const server = createServer((request, response) => {
    if (request.method !== "GET" || request.url !== "/metrics") {
      response.writeHead(404).end();
      return;
    }
    registry.metrics().then(
      (body) => response.writeHead(200, { "content-type": registry.contentType }).end(body),
      (error: Error) => response.writeHead(500).end(error.message),
    );
  });
  return server.listen(port);
}
