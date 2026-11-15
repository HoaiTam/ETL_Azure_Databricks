# Stack 90-sandbox (Sub C/D): workspace #2 cho DS. Chỉ dựng ở G3. Xem docs/devops/03, mục 5.

module "nt" {
  source = "../../modules/naming_tags"
  env    = "sandbox"
  owner  = var.member
  stack  = "90-sandbox"
}

resource "azurerm_resource_group" "sbx" {
  name     = "rg-instacart-sbx-${var.member}"
  location = var.location_c
  tags     = module.nt.tags
}

module "ws2" {
  source              = "../../modules/dbx_workspace"
  name                = "dbw-instacart-sbx-${var.member}"
  resource_group_name = azurerm_resource_group.sbx.name
  location            = azurerm_resource_group.sbx.location
  sku                 = "premium"
  tags                = module.nt.tags
}
