terraform {
  required_version = ">= 1.9"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 5.9"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.9"
    }
  }
}

provider "azurerm" {
  # subscription from ARM_SUBSCRIPTION_ID; azurerm 5 registers no resource providers unless listed
  resource_providers_to_register = [
    "Microsoft.Compute", "Microsoft.Network", "Microsoft.ContainerService", "Microsoft.ContainerRegistry",
  ]
  features {}
}
