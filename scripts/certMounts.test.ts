/**
 * The CA key is never mounted into a container or deployed (S8-5). Anyone with code execution in a container that
 * can read `ca.key` can mint a certificate for any gateway, so this guards every way it could get there:
 * - no Compose file mounts the certificate directory whole, `ca.key`, or the dynsec admin's key directory; the only
 *   directory mounted is `certs/fleet`, the simulated gateways';
 * - `pki.sh` with `PKI_OUT` (how dev-certs.sh issues the fleet) writes only certificates and keys there, never the
 *   CA's files, and leaves `ca.key` readable by its owner only;
 * - `deploy.sh` copies an explicit list of certificates without `ca.key`, and excludes both key directories.
 */
import { execFileSync } from "node:child_process";
import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, describe, expect, it } from "vitest";

const ROOT = join(import.meta.dirname, "..");
const DEPLOY = join(ROOT, "deploy");
const composeFiles = readdirSync(DEPLOY).filter((f) => /^compose.*\.ya?ml$/.test(f));

/** Every bind-mount source under ./certs or ./admin in a Compose file, short or long syntax. */
function certSources(text: string): string[] {
  return [...text.matchAll(/(?:^|[\s"'[,])(\.\/(?:certs|admin)[^:"'\s\],]*)(?=:)/gm)].map((m) => m[1] as string);
}

describe("certificate mounts (S8-5)", () => {
  it.each(composeFiles)("%s mounts only individual certificate files, or the fleet's directory, and never ca.key", (file) => {
    const sources = certSources(readFileSync(join(DEPLOY, file), "utf8"));
    for (const source of sources) {
      expect(source, `${file}: ${source}`).not.toMatch(/ca\.key|\/admin|^\.\/admin/);
      expect(source, `${file}: a whole certificate directory`).not.toMatch(/^\.\/certs\/?$/);
      const isFile = /\.(crt|key|pem)$/.test(source);
      expect(isFile || source === "./certs/fleet", `${file}: ${source} is a directory other than certs/fleet`).toBe(true);
    }
  });

  it("finds the mounts it guards (the parser isn't silently matching nothing)", () => {
    expect(certSources(readFileSync(join(DEPLOY, "compose.yaml"), "utf8"))).toEqual(expect.arrayContaining(["./certs/ca.crt", "./certs/mosquitto.key", "./certs/fleet"]));
    expect(certSources(`volumes: ["./certs:/certs:ro"]\n      - ./certs/ca.key:/x:ro`)).toEqual(["./certs", "./certs/ca.key"]);
  });

  const scratch = mkdtempSync(join(tmpdir(), "pki-guard-"));
  afterAll(() => rmSync(scratch, { recursive: true, force: true }));

  it("pki.sh issues the fleet into its own directory without the CA's files, and keeps ca.key private", () => {
    const ca = join(scratch, "ca");
    const fleet = join(scratch, "fleet");
    execFileSync(join(ROOT, "scripts", "pki.sh"), ["init", ca], { stdio: "ignore" });
    execFileSync(join(ROOT, "scripts", "pki.sh"), ["issue-fleet", ca, "sim-gw", "2"], { stdio: "ignore", env: { ...process.env, PKI_OUT: fleet } });
    expect(readdirSync(fleet).sort()).toEqual(["sim-gw-0001.crt", "sim-gw-0001.key", "sim-gw-0002.crt", "sim-gw-0002.key"]);
    expect(statSync(join(ca, "ca.key")).mode & 0o077, "ca.key readable by others").toBe(0);
  });

  it("deploy.sh copies no ca.key and excludes both key directories", () => {
    const script = readFileSync(join(ROOT, "scripts", "deploy.sh"), "utf8");
    const copied = [...script.matchAll(/certs(?:\+)?=\(([^)]*)\)/g)].flatMap((m) => (m[1] as string).split(/\s+/));
    expect(copied).toContain("ca.crt");
    expect(copied).not.toContain("ca.key");
    expect(script).toContain("--exclude 'deploy/certs'");
    expect(script).toContain("--exclude 'deploy/admin'");
  });
});
