/**
 * Checks the alerting setup, in three steps:
 * 1. `promtool check rules`: the rules file is valid;
 * 2. `promtool test rules`: the unit tests in rules.test.yml pass (synthetic series in, expected alerts out);
 * 3. every alert's `runbook` annotation points at a heading that exists in docs/runbooks.md (every alert has a
 *    runbook entry).
 * Needs Docker (promtool runs from the pinned Prometheus image). Exits non-zero on any failure.
 * Usage: `node scripts/check-alerts.ts`
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const ROOT = join(import.meta.dirname, "..");
const PROMETHEUS_IMAGE = "prom/prometheus:v3.15.0";

/** Runs promtool with `args` in deploy/prometheus, printing its output; throws if it fails. */
function promtool(args: string[]): void {
  const output = execFileSync("docker", ["run", "--rm", "-v", `${join(ROOT, "deploy", "prometheus")}:/p:ro`, "-w", "/p", "--entrypoint", "promtool", PROMETHEUS_IMAGE, ...args], { encoding: "utf8" });
  process.stdout.write(output);
}

/** The anchor GitHub-style Markdown gives a heading: lowercase, punctuation removed, spaces as hyphens. */
function anchorOf(heading: string): string {
  return heading.trim().toLowerCase().replace(/[^a-z0-9 -]/g, "").replace(/ /g, "-");
}

/** Returns the runbook links in the rules that don't match a heading in the runbook. */
function missingRunbookLinks(): string[] {
  const rules = readFileSync(join(ROOT, "deploy", "prometheus", "rules.yml"), "utf8");
  const runbook = readFileSync(join(ROOT, "docs", "runbooks.md"), "utf8");
  const anchors = new Set([...runbook.matchAll(/^## (.+)$/gm)].map((m) => anchorOf(m[1] ?? "")));
  const links = new Set([...rules.matchAll(/runbooks\.md#([a-z0-9-]+)/g)].map((m) => m[1] ?? ""));
  return [...links].filter((link) => !anchors.has(link));
}

promtool(["check", "rules", "rules.yml"]);
promtool(["test", "rules", "rules.test.yml"]);
const missing = missingRunbookLinks();
process.stdout.write(missing.length ? `runbook links without a heading: ${missing.join(", ")}\n` : "every alert's runbook link resolves\n");
process.exit(missing.length ? 1 : 0);
