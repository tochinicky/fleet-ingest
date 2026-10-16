/**
 * Deploy configuration that broke the first always-on deploy (S8-8), kept from coming back:
 * - the shared Node image is built by exactly one service, so parallel builds of one tag can't collide;
 * - no placeholder ACME email anywhere, and deploy.sh refuses to run without a real one.
 */
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

const ROOT = join(import.meta.dirname, "..");
const read = (path: string) => readFileSync(join(ROOT, path), "utf8");

describe("deploy configuration (S8-8)", () => {
  it("builds the Node image in exactly one service; the others never pull it", () => {
    const compose = read("deploy/compose.yaml");
    expect(compose.match(/dockerfile: Dockerfile/g)).toHaveLength(1);
    expect(compose).toMatch(/x-node: &node\n {2}image: fleet-ingest\/node:dev\n {2}pull_policy: never\n/);
    expect(compose).toMatch(/ {2}migrate:\n {4}<<: \*node\n {4}build: \{ context: \.\., dockerfile: Dockerfile \}\n {4}pull_policy: build\n/);
  });

  it("has no placeholder ACME email, and deploy.sh requires a real one", () => {
    for (const file of ["deploy/caddy/Caddyfile", "deploy/compose.production.yaml", "scripts/deploy.sh"]) {
      expect(read(file), file).not.toMatch(/admin@example\.invalid/);
    }
    expect(read("scripts/deploy.sh")).toMatch(/: "\$\{ACME_EMAIL:\?/);
    expect(read("deploy/compose.production.yaml")).toContain("CADDY_ACME_EMAIL: ${ACME_EMAIL:+email ${ACME_EMAIL}}");
  });
});
