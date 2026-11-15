# Stack 20-stream (Sub B): Event Hubs theo buổi. Xem docs/devops/03, mục 4.
# Bật/tắt bằng workflow stream-toggle; destroy mỗi đêm.
#
# [x] Provider Sub B + alias Sub A, remote state core, resource group
# [ ] Event Hubs namespace Standard (Kafka :9093), hub orders-stream, consumer group dbx-bronze
# [ ] SAS dbx-listen (listen) và sim-send (send)
# [ ] Secret eh-listen-connection-string, eh-send-connection-string, eh-bootstrap-server vào Key Vault core

data "terraform_remote_state" "core" {
  backend = "azurerm"
  config = {
    resource_group_name  = "rg-tfstate"
    storage_account_name = var.tfstate_account
    container_name       = "tfstate"
    key                  = "core.tfstate"
    use_azuread_auth     = true
  }
}

module "nt" {
  source = "../../modules/naming_tags"
  env    = "prod"
  owner  = "p3"
  stack  = "20-stream"
}

resource "random_string" "sfx" {
  length  = 5
  special = false
  upper   = false
}

resource "azurerm_resource_group" "stream" {
  name     = "rg-instacart-stream"
  location = var.location_b
  tags     = module.nt.tags
}
