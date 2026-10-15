resource "hcloud_ssh_key" "operator" {
  name       = "fleet-ingest-operator"
  public_key = var.ssh_public_key
}

# Only what the deployment serves is reachable: SSH for the operator, 80/443 for Caddy (API and Grafana over TLS),
# 8883 for gateways (mutual TLS, D6). Postgres, Prometheus and every other port stay on the VM's loopback.
resource "hcloud_firewall" "fleet" {
  name = "fleet-ingest"

  rule {
    description = "SSH from the operator only"
    direction   = "in"
    protocol    = "tcp"
    port        = "22"
    source_ips  = var.admin_cidrs
  }

  rule {
    description = "Caddy: ACME HTTP challenge and redirect to HTTPS"
    direction   = "in"
    protocol    = "tcp"
    port        = "80"
    source_ips  = ["0.0.0.0/0", "::/0"]
  }

  rule {
    description = "Caddy: API and Grafana over TLS"
    direction   = "in"
    protocol    = "tcp"
    port        = "443"
    source_ips  = ["0.0.0.0/0", "::/0"]
  }

  rule {
    description = "MQTT over mutual TLS for gateways (D6)"
    direction   = "in"
    protocol    = "tcp"
    port        = "8883"
    source_ips  = ["0.0.0.0/0", "::/0"]
  }
}

resource "hcloud_server" "fleet" {
  name         = "fleet-ingest"
  server_type  = var.server_type
  image        = var.image
  location     = var.location
  ssh_keys     = [hcloud_ssh_key.operator.id]
  firewall_ids = [hcloud_firewall.fleet.id]
  user_data    = file("${path.module}/cloud-init.yaml")

  labels = {
    project = "fleet-ingest"
    purpose = "lab"
  }

  public_net {
    ipv4_enabled = true
    ipv6_enabled = true
  }
}
