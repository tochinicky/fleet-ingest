# Everything in one resource group, so `terraform destroy` (or deleting the group) removes all of it.
resource "azurerm_resource_group" "fleet" {
  name     = "fleet-ingest-always-on"
  location = var.location
  tags     = local.tags
}

locals {
  tags = { project = "fleet-ingest", purpose = "always-on" }
}

resource "azurerm_virtual_network" "fleet" {
  name                = "fleet-ingest"
  resource_group_name = azurerm_resource_group.fleet.name
  location            = azurerm_resource_group.fleet.location
  address_space       = ["10.42.0.0/24"]
  tags                = local.tags
}

resource "azurerm_subnet" "fleet" {
  name                 = "fleet-ingest"
  resource_group_name  = azurerm_resource_group.fleet.name
  virtual_network_name = azurerm_virtual_network.fleet.name
  address_prefixes     = ["10.42.0.0/26"]
}

# Only what the deployment serves is reachable: SSH for the operator, 80/443 for Caddy (the API and Grafana over
# TLS), 8883 for gateways (mutual TLS, D6). Postgres, Prometheus and every other port stay on the VM's loopback, and
# the NSG's default rules deny all other inbound traffic.
resource "azurerm_network_security_group" "fleet" {
  name                = "fleet-ingest"
  resource_group_name = azurerm_resource_group.fleet.name
  location            = azurerm_resource_group.fleet.location
  tags                = local.tags

  security_rule {
    name                       = "ssh-from-operator"
    description                = "SSH from the operator only"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefixes    = var.admin_cidrs
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "caddy-http"
    description                = "Caddy: ACME HTTP challenge and redirect to HTTPS"
    priority                   = 110
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "80"
    source_address_prefix      = "Internet"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "caddy-https"
    description                = "Caddy: the API and Grafana over TLS"
    priority                   = 120
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "443"
    source_address_prefix      = "Internet"
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "mqtt-mtls"
    description                = "MQTT over mutual TLS for gateways (D6)"
    priority                   = 130
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "8883"
    source_address_prefix      = "Internet"
    destination_address_prefix = "*"
  }
}

resource "azurerm_subnet_network_security_group_association" "fleet" {
  subnet_id                 = azurerm_subnet.fleet.id
  network_security_group_id = azurerm_network_security_group.fleet.id
}

# Static, so the address gateways are configured with never changes; the DNS label gives it the one name Caddy
# gets a Let's Encrypt certificate for (no domain to buy).
resource "azurerm_public_ip" "fleet" {
  name                = "fleet-ingest"
  resource_group_name = azurerm_resource_group.fleet.name
  location            = azurerm_resource_group.fleet.location
  allocation_method   = "Static"
  sku                 = "Standard"
  domain_name_label   = var.dns_label
  tags                = local.tags
}

resource "azurerm_network_interface" "fleet" {
  name                = "fleet-ingest"
  resource_group_name = azurerm_resource_group.fleet.name
  location            = azurerm_resource_group.fleet.location
  tags                = local.tags

  ip_configuration {
    name                          = "primary"
    subnet_id                     = azurerm_subnet.fleet.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.fleet.id
  }
}

resource "azurerm_network_interface_security_group_association" "fleet" {
  network_interface_id      = azurerm_network_interface.fleet.id
  network_security_group_id = azurerm_network_security_group.fleet.id
}

# Always on: there is no auto-shutdown schedule (unlike the lab VM, which has a nightly one as a safety net).
resource "azurerm_linux_virtual_machine" "fleet" {
  name                            = "fleet-ingest"
  resource_group_name             = azurerm_resource_group.fleet.name
  location                        = azurerm_resource_group.fleet.location
  size                            = var.vm_size
  admin_username                  = "fleet"
  disable_password_authentication = true
  network_interface_ids           = [azurerm_network_interface.fleet.id]
  custom_data                     = base64encode(file("${path.module}/cloud-init.yaml"))
  tags                            = local.tags

  admin_ssh_key {
    username   = "fleet"
    public_key = var.ssh_public_key
  }

  os_disk {
    name                 = "fleet-ingest-os"
    caching              = "ReadWrite"
    storage_account_type = "StandardSSD_LRS"
    disk_size_gb         = var.disk_size_gb
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = local.arm64 ? "server-arm64" : "server"
    version   = "latest"
  }
}

locals {
  # The Arm sizes (the "p" in B2pls_v2) need the arm64 image; every image the stack uses has an arm64 build.
  arm64 = can(regex("^Standard_[A-Z][0-9]+p", var.vm_size))
}
