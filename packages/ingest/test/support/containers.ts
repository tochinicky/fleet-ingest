/**
 * Test containers shared by the integration tests: Postgres with the repo's migrations applied, and a Mosquitto broker
 * on the exact image D2 pins.
 */
import { execFileSync } from "node:child_process";
import { rmSync } from "node:fs";
import { PostgreSqlContainer } from "@testcontainers/postgresql";
import type { StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import { GenericContainer, Wait } from "testcontainers";
import type { StartedTestContainer } from "testcontainers";
import pg from "pg";
import { applyMigrations, loadMigrations, MIGRATIONS_DIR } from "@fleet-ingest/db";

export const POSTGRES_IMAGE = "postgres:17.11-alpine";
export const MOSQUITTO_IMAGE = "eclipse-mosquitto:2.1.2-alpine";

/**
 * Removes a host directory a broker container wrote into. Mosquitto drops to its own user (uid 1883) inside the
 * container, so on a Linux host the files it creates in a bind-mounted directory belong to uid 1883, and the test's
 * own user may not delete them: a plain `rmSync` failed with EPERM on the CI runner. Docker Desktop remaps ownership,
 * so the Mac never showed it. The contents are removed from inside a short-lived root container (the broker image,
 * already pulled), then the now-empty directory from the host. Running the broker as the host's uid instead would
 * have changed the process whose persistence behaviour these tests pin.
 */
export function removeBrokerDataDir(dir: string): void {
  try {
    execFileSync("docker", ["run", "--rm", "--user", "0", "--entrypoint", "sh", "-v", `${dir}:/d`, MOSQUITTO_IMAGE, "-c", "rm -rf /d/* /d/.[!.]* 2>/dev/null; true"], { stdio: "ignore" });
  } catch {
    // Docker unavailable: fall through, and let rmSync report what it can't remove.
  }
  rmSync(dir, { recursive: true, force: true });
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
