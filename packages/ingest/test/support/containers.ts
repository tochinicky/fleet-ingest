/**
 * Test containers shared by the integration tests: Postgres with the repo's migrations applied, and a Mosquitto broker
 * on the exact image D2 pins.
 */
import { execFileSync } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import type { StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { GenericContainer, Wait } from "testcontainers";
import type { StartedTestContainer } from "testcontainers";
import pg from "pg";
import { applyMigrations, loadMigrations, MIGRATIONS_DIR } from "@fleet-ingest/db";

export const POSTGRES_IMAGE = "postgres:17.11-alpine";
export const MOSQUITTO_IMAGE = "eclipse-mosquitto:2.1.2-alpine";

/**
 * A host directory for a broker container's data, and how to remove it afterwards (found by CI on Linux).
 *
 * The broker image's entrypoint, running as root, does `chown -R 1883:1883 /mosquitto/data`, so on a Linux host the
 * bind-mounted directory itself and everything Mosquitto writes in it end up owned by uid 1883. Two things then
 * stop the test's own user from deleting it: the files aren't theirs, and the temp directory is sticky (`/tmp` is
 * `drwxrwxrwt`), where only a directory's owner may remove it. Docker Desktop remaps ownership, so the Mac never
 * showed either.
 *
 * So the mounted directory is `data` inside a parent the test owns (`mkdtemp`, not sticky), and cleanup mounts the
 * *parent* into a short-lived root container on the same image (already pulled) and empties it, `data` included.
 * The parent is then empty and the test's own, and a plain `rmSync` removes it. No uid has to be passed in, and it
 * works whatever the image's PUID is. Running the broker as the host's uid instead would have changed the process
 * whose persistence behaviour the tests pin.
 */
export function makeBrokerDataDir(prefix: string): { parent: string; data: string } {
  const parent = mkdtempSync(join(tmpdir(), prefix));
  const data = join(parent, "data");
  mkdirSync(data);
  chmodSync(data, 0o777);
  return { parent, data };
}

/** Removes a directory made by {@link makeBrokerDataDir}, whoever owns what the broker wrote into it. */
export function removeBrokerDataDir(parent: string): void {
  try {
    execFileSync("docker", ["run", "--rm", "--user", "0", "--entrypoint", "sh", "-v", `${parent}:/p`, MOSQUITTO_IMAGE, "-c", "rm -rf /p/* /p/.[!.]* 2>/dev/null; true"], { stdio: "ignore" });
  } catch {
    // Docker unavailable: fall through, and let rmSync report what it can't remove.
  }
  rmSync(parent, { recursive: true, force: true });
}

/** Starts Postgres 17 and applies every migration in `db/migrations`. */
export async function startMigratedPostgres(): Promise<StartedPostgreSqlContainer> {
  const container = await new PostgreSqlContainer(POSTGRES_IMAGE).start();
  const client = new pg.Client({ connectionString: container.getConnectionUri() });
  await client.connect();
  await applyMigrations(client, loadMigrations(MIGRATIONS_DIR));
  await client.end();
  return container;
}

/** Starts Mosquitto with anonymous access on 1883. Returns the container and its `mqtt://` URL. */
export async function startMosquitto(extraConfig: string[] = []): Promise<{ container: StartedTestContainer; url: string }> {
  const config = ["listener 1883", "allow_anonymous true", "log_dest stdout", ...extraConfig, ""].join("\n");
  const container = await new GenericContainer(MOSQUITTO_IMAGE)
    .withCopyContentToContainer([{ content: config, target: "/mosquitto/config/mosquitto.conf" }])
    .withExposedPorts(1883)
    .withWaitStrategy(Wait.forLogMessage(/running/))
    .start();
  return { container, url: `mqtt://${container.getHost()}:${container.getMappedPort(1883)}` };
}
