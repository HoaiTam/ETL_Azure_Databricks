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

  # Lần apply đầu chạy với local state (chưa có storage để chứa state).
  # Apply xong: bỏ comment dòng dưới rồi chạy
  #   terraform init -backend-config=../backend.hcl -backend-config="key=bootstrap.tfstate" -migrate-state
  # backend "azurerm" {}
}

provider "azurerm" {
  features {}
  subscription_id     = var.subscription_id_a
  storage_use_azuread = true
}
