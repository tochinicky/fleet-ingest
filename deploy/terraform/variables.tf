variable "subscription_id" {
  description = "The Azure subscription to create the resources in. Pass it as TF_VAR_subscription_id (or ARM_SUBSCRIPTION_ID), never in a committed file."
  type        = string
  sensitive   = true
}

variable "location" {
  description = "Azure region. The lab VM's region by default; prices were compared there (decisions.md S8-6)."
  type        = string
  default     = "germanywestcentral"
}

variable "vm_size" {
  description = "VM size. No default: the size and its monthly price are part of the spend approval (decisions.md S8-6 has the measured fit and the prices)."
  type        = string
}

variable "dns_label" {
  description = "The public IP's DNS label: the VM is <label>.<location>.cloudapp.azure.com, the one name Caddy gets a certificate for and gateways connect to."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,61}[a-z0-9]$", var.dns_label))
    error_message = "A DNS label is 3-63 lowercase letters, digits and hyphens, starting with a letter."
  }
}

variable "ssh_public_key" {
  description = "The operator's SSH public key (the contents, not a path). The only way in: password login is off."
  type        = string
}

variable "admin_cidrs" {
  description = "Source ranges allowed to reach SSH. Gateways (8883) and the browser-facing Caddy (80, 443) are open to all."
  type        = list(string)

  validation {
    condition     = length(var.admin_cidrs) > 0 && !contains(var.admin_cidrs, "0.0.0.0/0") && !contains(var.admin_cidrs, "*")
    error_message = "List at least one admin range, and not 0.0.0.0/0: SSH is never open to the internet."
  }
}

variable "disk_size_gb" {
  description = "OS disk size (Standard SSD). 32 GB is the smallest managed-disk tier; 30 days of readings for a few gateways is far below it."
  type        = number
  default     = 32
}
