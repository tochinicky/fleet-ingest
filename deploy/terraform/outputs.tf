output "ipv4" {
  description = "The VM's static public IPv4 address."
  value       = azurerm_public_ip.fleet.ip_address
}

output "fqdn" {
  description = "The VM's name from the public IP's DNS label: Caddy's certificate is for it, and gateways connect to it on 8883."
  value       = azurerm_public_ip.fleet.fqdn
}

output "ssh" {
  description = "How to log in."
  value       = "ssh fleet@${azurerm_public_ip.fleet.fqdn}"
}

output "urls" {
  description = "What Caddy serves on the one name: the API at the root, Grafana under /grafana/."
  value = {
    api     = "https://${azurerm_public_ip.fleet.fqdn}/health/ready"
    grafana = "https://${azurerm_public_ip.fleet.fqdn}/grafana/"
  }
}
