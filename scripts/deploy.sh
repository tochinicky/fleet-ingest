#!/usr/bin/env bash
# Deploys the production profile (D8, S8-6) to the Azure VM that deploy/terraform created. Run after the apply: it
# copies the repo and the service certificates, then starts the stack behind Caddy.
#
#   API_TOKEN=... GRAFANA_ADMIN_PASSWORD=... [ACME_EMAIL=...] scripts/deploy.sh "$(terraform -chdir=deploy/terraform output -raw fqdn)"
#
# - The lab CA's private key, the dynsec admin's key and the simulated gateways' keys never leave the laptop
#   (deploy/certs and deploy/admin are excluded from the copy, S8-5). The VM gets the CA certificate, the CRL, the
#   broker's key pair and one key pair per service (ingest-1, ingest-2, api, ops), by name.
# - The secrets go to deploy/production.env on the VM (mode 0600, through stdin), never onto a command line.
# - One name: the public IP's DNS label (<label>.<region>.cloudapp.azure.com). Caddy gets a Let's Encrypt certificate
#   for it and serves the API at its root and Grafana under /grafana/.
# - Gateways connect to <fqdn>:8883 with TLS server name `mosquitto`: the broker certificate's SAN
#   (home-energy-gateway: Uplink:TlsServerName).
set -euo pipefail
fqdn="${1:?usage: scripts/deploy.sh <vm-fqdn>}"
: "${API_TOKEN:?set API_TOKEN (at least 16 characters)}"
: "${GRAFANA_ADMIN_PASSWORD:?set GRAFANA_ADMIN_PASSWORD}"
root="$(cd "$(dirname "$0")/.." && pwd)"
target="fleet@$fqdn"

rsync -az --delete \
  --exclude .git --exclude node_modules --exclude 'spikes/**/data' \
  --exclude 'deploy/certs' --exclude 'deploy/admin' --exclude 'deploy/terraform' --exclude 'deploy/production.env' \
  "$root/" "$target:/opt/fleet-ingest/"

certs=(ca.crt crl.pem mosquitto.crt mosquitto.key)
for cn in ingest-1 ingest-2 api ops; do certs+=("$cn.crt" "$cn.key"); done
ssh "$target" "mkdir -p /opt/fleet-ingest/deploy/certs && chmod 700 /opt/fleet-ingest/deploy/certs"
(cd "$root/deploy/certs" && rsync -az "${certs[@]}" "$target:/opt/fleet-ingest/deploy/certs/")

printf '%s\n' "PUBLIC_BIND=0.0.0.0" "PUBLIC_HOST=$fqdn" "ACME_EMAIL=${ACME_EMAIL:-admin@example.invalid}" \
  "API_TOKEN=$API_TOKEN" "GRAFANA_ADMIN_PASSWORD=$GRAFANA_ADMIN_PASSWORD" |
  ssh "$target" "umask 077 && cat > /opt/fleet-ingest/deploy/production.env"
ssh "$target" "cd /opt/fleet-ingest && docker compose --env-file deploy/production.env -f deploy/compose.yaml -f deploy/compose.production.yaml up -d --build"

echo "API:     https://$fqdn/health/ready"
echo "Grafana: https://$fqdn/grafana/"
echo "MQTT:    $fqdn:8883 (mutual TLS, server name mosquitto)"
