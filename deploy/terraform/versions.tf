# D8 (changed in S8-6): one small Azure VM running the same Compose stack, always on. Local state on purpose: one VM,
# one operator. `plan` needs Azure credentials but changes nothing; `apply` creates billed resources and is run only
# by the operator who holds the subscription, after the spend is approved.
terraform {
  required_version = ">= 1.9.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "4.14.0"
    }
  }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}
