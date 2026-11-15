# Các stack sau đọc output qua terraform_remote_state (key = core.tfstate).
# Thêm dần theo docs/devops/03, bước 3.10.

output "location" {
  value = azurerm_resource_group.core.location
}

output "rg_name" {
  value = azurerm_resource_group.core.name
}
