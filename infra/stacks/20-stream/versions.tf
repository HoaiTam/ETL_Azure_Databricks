terraform {
  required_version = "~> 1.9"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  backend "azurerm" {
    key = "stream.tfstate"
  }
}

# Sub B: nơi tạo Event Hubs.
provider "azurerm" {
  features {}
  subscription_id = var.subscription_id_b
}

# Sub A: chỉ để ghi SAS của Event Hubs vào Key Vault core.
provider "azurerm" {
  alias = "core"
  features {}
  subscription_id = var.subscription_id_a
}
