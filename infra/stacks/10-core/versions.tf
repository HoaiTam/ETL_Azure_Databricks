terraform {
  required_version = "~> 1.9"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }

  # Phần còn lại lấy từ infra/backend.hcl:
  #   terraform init -backend-config=../../backend.hcl
  backend "azurerm" {
    key = "core.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
  }
  subscription_id     = var.subscription_id_a
  storage_use_azuread = true
}
