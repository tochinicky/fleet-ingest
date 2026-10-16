# One always-on VM on Azure (D8, changed in S8-6)

One VM running the same Compose stack (the production profile), with Caddy in front of the API and Grafana. Everything is in one resource group, `fleet-ingest-always-on`: a VNet and subnet, a network security group, a static Standard public IP with a DNS label, a NIC, and the VM with a Standard SSD OS disk. There is no auto-shutdown: it's always on.

**`terraform apply` creates billed resources.** The size and its monthly price come with the spend approval; decisions.md S8-6 has the measured fit and the prices. `vm_size` has no default for that reason.

## What is reachable

| Port | From | What |
|---|---|---|
| 22 | `admin_cidrs` only (never 0.0.0.0/0, validated) | SSH, key only |
| 80 | anywhere | Caddy: the ACME HTTP challenge, and a redirect to HTTPS |
| 443 | anywhere | Caddy: the API at `/`, Grafana at `/grafana/` |
| 8883 | anywhere | MQTT over mutual TLS (D6): a client without a certificate from the lab CA fails the handshake |

Everything else (Postgres, Prometheus, Alertmanager, the metrics ports) stays on the VM's loopback or Docker network, and the NSG's default rules deny any other inbound traffic.

## Plan

Always with an explicit subscription: the provider is given it, so it never falls back to whatever `az account` has as default.

```sh
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars    # gitignored: vm_size, dns_label, ssh_public_key, admin_cidrs
az login                                        # any account with access to the subscription
export ARM_SUBSCRIPTION_ID=<the subscription id>
export TF_VAR_subscription_id="$ARM_SUBSCRIPTION_ID"
terraform init
terraform plan -out=fleet.tfplan
terraform show fleet.tfplan | grep -E '^  # |vm_size|size  |domain_name_label|location'   # what it will create
```

The plan should show 9 resources to add and nothing to change or destroy: the resource group, VNet, subnet, NSG and its association, public IP, NIC and its NSG association, and the VM.

## Apply

```sh
terraform apply fleet.tfplan
terraform output          # ipv4, fqdn, ssh, urls
# about 2 minutes for cloud-init (Docker, the fleet user in the docker group)
API_TOKEN=... GRAFANA_ADMIN_PASSWORD=... ACME_EMAIL=... scripts/deploy.sh "$(terraform output -raw fqdn)"
```

`scripts/deploy.sh` copies the repo without `deploy/certs` and `deploy/admin`, then **only** the service certificates by name: never the CA's private key, the dynsec admin's key or the simulated gateways' keys (S8-5). It writes the secrets to a 0600 env file on the VM and starts the stack with `deploy/compose.production.yaml`.

Connect a real gateway: `Uplink:Host` = the `fqdn` output, port 8883, `Uplink:TlsServerName` = `mosquitto`, with its own certificate from `node scripts/gateway-admin.ts enrol <id>`.

## Destroy

```sh
terraform destroy         # with the same ARM_SUBSCRIPTION_ID and TF_VAR_subscription_id
```

This deletes the resource group and everything in it: the VM, its disk with the Postgres and broker volumes, the public IP (its address and DNS label are released), the NSG and the network. Nothing is backed up first. Take a `pg_dump` and the dynsec state (docs/runbooks.md) over SSH before destroying if the data matters.

## Run the production profile locally

```sh
API_TOKEN=local-prod-token-0123 GRAFANA_ADMIN_PASSWORD=change-me \
  docker compose -f deploy/compose.yaml -f deploy/compose.production.yaml up -d --build
curl -sk https://localhost/health/ready          # Caddy's internal CA locally, hence -k
```

## Why not Hetzner any more

Slice 7's plan-only module targeted Hetzner Cloud. S8-6 moved the always-on deployment to Azure, inside the existing credit, and removed the Hetzner module rather than keep a second provider that nothing applies and that would drift unnoticed.
