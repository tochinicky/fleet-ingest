output "ipv4" {
  description = "The VM's public IPv4 address."
  value       = hcloud_server.fleet.ipv4_address
}

output "ssh" {
  description = "How to log in (cloud-init creates the fleet user)."
  value       = "ssh fleet@${hcloud_server.fleet.ipv4_address}"
}

output "hosts" {
  description = "Hostnames Caddy serves, through sslip.io's wildcard DNS, so no domain is needed. scripts/deploy.sh uses them."
  value = {
    api     = "api.${replace(hcloud_server.fleet.ipv4_address, ".", "-")}.sslip.io"
    grafana = "grafana.${replace(hcloud_server.fleet.ipv4_address, ".", "-")}.sslip.io"
  }
}
