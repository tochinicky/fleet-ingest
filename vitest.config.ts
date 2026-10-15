/**
 * Test projects: `unit` covers the pure core and runs in milliseconds; `integration` covers the adapters against real
 * Postgres and Mosquitto containers (Testcontainers), so it needs Docker.
 */
import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    projects: [
      { test: { name: "unit", include: ["packages/*/test/**/*.test.ts", "scripts/**/*.test.ts"], exclude: ["**/*.integration.test.ts", "**/node_modules/**"] } },
      {
        test: {
          name: "integration",
          include: ["packages/*/test/**/*.integration.test.ts"],
          testTimeout: 120_000,
          hookTimeout: 180_000,
          fileParallelism: false,
        },
      },
    ],
  },
});
