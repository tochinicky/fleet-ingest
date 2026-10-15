# One image for every Node service (ingest, api, fleet-sim, migrate); Compose picks the entry point.
# Node 24 runs the TypeScript sources directly (type stripping), so there is no build step.

FROM node:24.21.0-slim AS deps
WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY packages/contract/package.json packages/contract/
COPY packages/db/package.json packages/db/
COPY packages/ingest/package.json packages/ingest/
COPY packages/api/package.json packages/api/
COPY packages/fleet-sim/package.json packages/fleet-sim/
COPY packages/observability/package.json packages/observability/
COPY packages/broker-exporter/package.json packages/broker-exporter/
COPY packages/alert-logger/package.json packages/alert-logger/
COPY spikes/mqtt-acks/package.json spikes/mqtt-acks/
COPY scripts/package.json scripts/
COPY tools/lint/package.json tools/lint/
RUN COREPACK_ENABLE_DOWNLOAD_PROMPT=0 pnpm install --frozen-lockfile --prod --filter "./packages/**"

FROM node:24.21.0-slim
WORKDIR /app
ENV NODE_ENV=production
COPY --from=deps /app ./
COPY packages ./packages
COPY db ./db
USER node
