variable "hcloud_token" {
  description = "Hetzner Cloud API token (read and write) for the lab project. Pass it as TF_VAR_hcloud_token, never in a file."
  type        = string
  sensitive   = true

  validation {
    condition     = length(var.hcloud_token) == 64
    error_message = "A Hetzner Cloud API token is 64 characters."
  }
}

variable "server_type" {
  description = "Server type. No default: the size and its monthly price are part of the D8 approval. The stack with 500 simulated gateways saturated 8 CPU / 4 GB (decisions.md S6-6); without fleet-sim, 4 vCPU / 8 GB is the floor to ask for."
  type        = string
}

variable "location" {
  description = "Hetzner location, e.g. fsn1, nbg1 or hel1."
  type        = string
  default     = "fsn1"
}

variable "image" {
  description = "OS image. Ubuntu 24.04 ships docker.io and docker-compose-v2 in its own archive, so cloud-init needs no third-party repository."
  type        = string
  default     = "ubuntu-24.04"
}

variable "ssh_public_key" {
  description = "The operator's SSH public key (the contents, not a path). The only way in."
  type        = string
}

variable "admin_cidrs" {
  description = "Source ranges allowed to reach SSH. Gateways (8883) and the browser-facing Caddy (80, 443) are open to all."
  type        = list(string)

  validation {
    condition     = length(var.admin_cidrs) > 0 && !contains(var.admin_cidrs, "0.0.0.0/0")
    error_message = "List at least one admin range, and not 0.0.0.0/0: SSH is never open to the internet."
  }
}
