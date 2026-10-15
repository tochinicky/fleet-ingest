# D8: one small Hetzner Cloud VM running the same Compose stack. Local state on purpose: one VM, one operator, and
# nothing applied without an approved spend. `plan` is free; `apply` creates a billed server.
terraform {
  required_version = ">= 1.9.0"

  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "1.69.0"
    }
  }
}

provider "hcloud" {
  token = var.hcloud_token
}
