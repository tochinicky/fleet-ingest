#!/usr/bin/env bash
# Deploys the production profile (D8) to the VM that deploy/terraform created. Run only after an approved
# `terraform apply`: it copies the repo and the service certificates, then starts the stack behind Caddy.
#
#   API_TOKEN=... GRAFANA_ADMIN_PASSWORD=... scripts/deploy.sh <vm-ipv4>
#
# - The lab CA's private key and the simulated gateways' keys never leave the laptop. The VM gets the CA certificate,
#   the CRL, the broker's key pair and one key pair per service (ingest-1, ingest-2, api, ops).
# - The secrets go to deploy/production.env on the VM (mode 0600, through stdin), never onto a command line.
# - Hostnames are sslip.io names for the VM's address, so Caddy gets public certificates without a domain.
# - Gateways connect to <vm-ipv4>:8883 with TLS server name `mosquitto`: the broker certificate's SAN
#   (home-energy-gateway: Uplink:TlsServerName).
set -euo pipefail
ip="${1:?usage: scripts/deploy.sh <vm-ipv4>}"
: "${API_TOKEN:?set API_TOKEN (at least 16 characters)}"
: "${GRAFANA_ADMIN_PASSWORD:?set GRAFANA_ADMIN_PASSWORD}"
root="$(cd "$(dirname "$0")/.." && pwd)"
dashed="${ip//./-}"
target="fleet@$ip"

rsync -az --delete \
  --exclude .git --exclude node_modules --exclude 'spikes/**/data' \
  --exclude 'deploy/certs' --exclude 'deploy/terraform' --exclude 'deploy/production.env' \
  "$root/" "$target:/opt/fleet-ingest/"

certs=(ca.crt crl.pem mosquitto.crt mosquitto.key)
for cn in ingest-1 ingest-2 api ops; do certs+=("$cn.crt" "$cn.key"); done
ssh "$target" "mkdir -p /opt/fleet-ingest/deploy/certs && chmod 700 /opt/fleet-ingest/deploy/certs"
(cd "$root/deploy/certs" && rsync -az "${certs[@]}" "$target:/opt/fleet-ingest/deploy/certs/")

printf '%s\n' "PUBLIC_BIND=0.0.0.0" "API_HOST=api.$dashed.sslip.io" "GRAFANA_HOST=grafana.$dashed.sslip.io" \
  "API_TOKEN=$API_TOKEN" "GRAFANA_ADMIN_PASSWORD=$GRAFANA_ADMIN_PASSWORD" |
  ssh "$target" "umask 077 && cat > /opt/fleet-ingest/deploy/production.env"
ssh "$target" "cd /opt/fleet-ingest && docker compose --env-file deploy/production.env -f deploy/compose.yaml -f deploy/compose.production.yaml up -d --build"

echo "API:     https://api.$dashed.sslip.io/health/ready"
echo "Grafana: https://grafana.$dashed.sslip.io/"
echo "MQTT:    $ip:8883 (mutual TLS, server name mosquitto)"
