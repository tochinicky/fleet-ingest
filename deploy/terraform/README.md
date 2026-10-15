# One VM on Hetzner Cloud (D8)

One server running the same Compose stack, with Caddy in front of the API and Grafana. It creates three things: an SSH key, a firewall, and the server.

**`terraform apply` creates a billed server. It runs only once the spend is approved (D8).** Until then, this stops at `plan`, and the production profile runs locally (see "Run the production profile locally" below).

## Plan (free)

```sh
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars   # gitignored; fill in server_type, ssh_public_key, admin_cidrs
export TF_VAR_hcloud_token=...                  # a read/write token for the lab project
terraform init
terraform plan -out=fleet.tfplan
```

- **`server_type` has no default.** The size and its monthly price are what the approval is about. The stack saturated an 8 CPU / 4 GB VM with 500 simulated gateways (decisions.md S6-6). Without fleet-sim, ask for at least 4 vCPU / 8 GB. Check the current types and prices with `hcloud server-type list` or the console before asking.
- `plan` makes no API calls for these resources, so it also runs with a placeholder token. That's how slice 7 ran it, before any approval. A real-token plan is the one to show at approval: it's the first one that checks the server type exists in the location.

## Apply (after approval)

```sh
terraform apply fleet.tfplan
terraform output          # ipv4, ssh, hosts
# wait about 2 minutes for cloud-init (Docker, the fleet user)
API_TOKEN=... GRAFANA_ADMIN_PASSWORD=... scripts/deploy.sh "$(terraform output -raw ipv4)"
```

`scripts/deploy.sh` copies the repo and **only** the service certificates (never the CA's private key or the simulated gateways' keys). It writes the secrets to a 0600 env file on the VM and starts the stack with `deploy/compose.production.yaml`.

Connect a real gateway: `Uplink:Host` = the VM's address, port 8883, `Uplink:TlsServerName` = `mosquitto`, with its own certificate from `scripts/pki.sh`.

## Destroy

```sh
terraform destroy
```

This deletes the server, its disk with the Postgres and broker volumes, the firewall and the key. Nothing is backed up first: it's a lab. Take a `pg_dump` over SSH before destroying if the data matters.

## Run the production profile locally

```sh
API_TOKEN=local-prod-token-0123 GRAFANA_ADMIN_PASSWORD=change-me \
  docker compose -f deploy/compose.yaml -f deploy/compose.production.yaml up -d --build
curl -sk https://api.localhost/health/ready      # Caddy's internal CA locally, hence -k
```
