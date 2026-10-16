/**
 * The API's pool bounds every query (S7-10), and a timeout is reported as the database being slow, not as a bug:
 * the health job counts it as `reason="timeout"` and stops the pass, and a request gets 503. The deliberate-break
 * check `api-timeouts-removed` deletes the timeouts and requires this file to fail.
 */
import type pg from "pg";
import { describe, expect, it } from "vitest";
import { createRegistry } from "@fleet-ingest/observability";
import { isDatabaseTimeout, openApiPool, QUERY_TIMEOUT_MS, STATEMENT_TIMEOUT_MS } from "../src/databasePool.ts";
import { startHealthJob } from "../src/healthJob.ts";

describe("openApiPool", () => {
  it("sets a 10 s statement timeout, a longer 15 s query timeout, TCP keepalive and a connect timeout", async () => {
    const pool = openApiPool("postgres://fleet:x@127.0.0.1:1/fleet", () => undefined);
    const options = pool.options;
    await pool.end();
    expect(options).toMatchObject({ statement_timeout: 10_000, query_timeout: 15_000, keepAlive: true, connectionTimeoutMillis: 5000, max: 5 });
    expect(QUERY_TIMEOUT_MS).toBeGreaterThan(STATEMENT_TIMEOUT_MS);
  });
});

describe("isDatabaseTimeout", () => {
  it.each([
    [{ code: "57014" }, true],
    [new Error("Query read timeout"), true],
    [new Error("timeout exceeded when trying to connect"), true],
    [{ code: "57P01" }, false],
    [new Error("Connection terminated unexpectedly"), false],
    [new Error("relation \"gateways\" does not exist"), false],
    [undefined, false],
  ])("%j → %s", (error, expected) => {
    expect(isDatabaseTimeout(error)).toBe(expected);
  });
});

describe("health job on a database that times out", () => {
  it("counts the failure as a timeout, stops the pass, and stamps neither the part nor the pass as done", async () => {
    const registry = createRegistry("api-test");
    const timeout = Object.assign(new Error("canceling statement due to statement timeout"), { code: "57014" });
    const pool = { query: () => Promise.reject(timeout) } as unknown as pg.Pool;
    const errors: unknown[] = [];
    const startedAt = Date.now() / 1000;
    const job = startHealthJob(pool, registry, (error) => errors.push(error));
    await new Promise((resolve) => setTimeout(resolve, 50));
    job.stop();
    const text = await registry.metrics();
    const healthStamp = Number(/fleet_health_job_part_last_success_timestamp_seconds\{part="health",service="api-test"\} ([\d.e+]+)/.exec(text)?.[1]);

    expect(errors).toContain(timeout);
    expect(text).toMatch(/fleet_health_job_failures_total\{part="health",reason="timeout",service="api-test"\} 1/);
    expect(healthStamp, "the part's stamp is still the job's start: it never succeeded").toBeLessThanOrEqual(startedAt + 0.01);
    expect(text).toMatch(/fleet_health_job_last_run_timestamp_seconds\{service="api-test"\} 0/);
    expect(text, "every pass is in the histogram, failed ones too: a slow pass can't fall between scrapes (S8-3)")
      .toMatch(/fleet_health_job_pass_duration_seconds_count\{[^}]*part="health"[^}]*\} 1/);
    expect(text).toMatch(/fleet_health_gaps_warm\{service="api-test"\} 0/);
    expect(job.fleetGaps()).toEqual({ complete: false, asOf: null });
  });
});
