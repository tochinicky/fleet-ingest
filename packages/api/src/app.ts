/**
 * Builds the API's HTTP app from its dependencies, without listening, so tests can drive it with
 * `inject`. Health endpoints are open; everything else needs the static bearer token (user auth is a non-goal).
 */
import { timingSafeEqual } from "node:crypto";
import Fastify from "fastify";
import type { FastifyInstance, FastifyRequest } from "fastify";
import type pg from "pg";
import { gatewayCommandSchema } from "@fleet-ingest/contract";
import type { CommandPublisher } from "./commandPublisher.ts";
import type { CommandReconciler } from "./commandReconciler.ts";
import { encodeCursor, parseReadingQuery } from "./core/readingQuery.ts";
import { isDatabaseTimeout } from "./databasePool.ts";
import type { FleetGapsReport } from "./healthSnapshot.ts";
import { deviceGapsReport, fleetReport, gatewayReport } from "./healthReport.ts";
import { databaseReachable, deviceState, readingsPage, setDesiredMode } from "./queries.ts";

/** What the app needs from the outside world. */
export interface AppDependencies {
  pool: pg.Pool;
  /** Reports whether the broker connection is up, for `/health/ready`. */
  commands: CommandPublisher;
  /** Publishes desired modes, in order with its own reconcile passes (S7-1). */
  reconciler: Pick<CommandReconciler, "publish">;
  /** The fleet's open gaps as of the health worker's last gaps pass, with their age (S7-11, S8-3). */
  fleetGaps: (now: Date) => Promise<FleetGapsReport>;
  apiToken: string;
  /** The clock, injected so tests can pin "now" for default time windows and health ages. */
  now: () => Date;
  logger: boolean | { level: string };
}

/** Compares the presented bearer token with the configured one in constant time, so timing reveals nothing. */
function hasValidToken(request: FastifyRequest, expected: string): boolean {
  const header = request.headers.authorization ?? "";
  const presented = Buffer.from(header.startsWith("Bearer ") ? header.slice(7) : "");
  const wanted = Buffer.from(expected);
  return presented.length === wanted.length && timingSafeEqual(presented, wanted);
}

/** Creates the app and registers every route. */
export function buildApp(deps: AppDependencies): FastifyInstance {
  const app = Fastify({ logger: deps.logger });

  // A database that didn't answer in time is the database being slow or away, not a bug: 503 (S7-10). Anything else
  // goes to Fastify's default handler, which keeps its own status codes and answers 500 for the rest.
  app.setErrorHandler((error, _request, reply) => {
    if (isDatabaseTimeout(error)) return reply.code(503).send({ error: "the database didn't answer in time" });
    return reply.send(error);
  });

  app.get("/health/live", async () => ({ status: "ok" }));

  app.get("/health/ready", async (_request, reply) => {
    const database = await databaseReachable(deps.pool);
    const broker = deps.commands.connected();
    const ready = database && broker;
    return reply.code(ready ? 200 : 503).send({ status: ready ? "ready" : "not ready", database, broker });
  });

  app.register(async (secured) => {
    secured.addHook("onRequest", async (request, reply) => {
      if (!hasValidToken(request, deps.apiToken)) return reply.code(401).send({ error: "missing or invalid bearer token" });
    });

    secured.get("/fleet", async () => {
      const now = deps.now();
      return fleetReport(deps.pool, now, await deps.fleetGaps(now));
    });

    secured.get<{ Params: { id: string } }>("/gateways/:id", async (request, reply) => {
      const found = await gatewayReport(deps.pool, request.params.id, deps.now());
      return found ?? reply.code(404).send({ error: "unknown gateway" });
    });

    secured.put<{ Params: { id: string } }>("/gateways/:id/mode", async (request, reply) => {
      const command = gatewayCommandSchema.safeParse(request.body);
      if (!command.success) return reply.code(400).send({ error: 'body must be {"mode": "surplus" | "max" | "off"}' });
      if (!(await setDesiredMode(deps.pool, request.params.id, command.data.mode, deps.now()))) return reply.code(404).send({ error: "unknown gateway" });
      const published = await deps.reconciler.publish(request.params.id, command.data.mode);
      return reply.code(202).send({
        gatewayId: request.params.id,
        mode: command.data.mode,
        delivery: published ? "stored and published retained: applied when the gateway is connected" : "stored; the broker is unreachable, so the reconciler publishes it when it's back",
      });
    });

    secured.get<{ Params: { id: string } }>("/devices/:id/state", async (request, reply) => {
      const state = await deviceState(deps.pool, request.params.id);
      return state ?? reply.code(404).send({ error: "no readings for this device" });
    });

    secured.get<{ Params: { id: string } }>("/devices/:id/readings", async (request, reply) => {
      const query = parseReadingQuery(request.query, deps.now());
      if ("error" in query) return reply.code(400).send({ error: query.error });
      const page = await readingsPage(deps.pool, request.params.id, query);
      return { items: page.items, nextCursor: page.next ? encodeCursor(page.next) : null };
    });

    secured.get<{ Params: { id: string } }>("/devices/:id/gaps", async (request, reply) => {
      const query = parseReadingQuery(request.query, deps.now());
      if ("error" in query) return reply.code(400).send({ error: query.error });
      const report = await deviceGapsReport(deps.pool, request.params.id, query.from, query.to, deps.now());
      return report ?? reply.code(404).send({ error: "unknown device" });
    });
  });

  return app;
}
