# Cấu hình backend dùng chung cho mọi stack. Không chứa secret nên được commit.
# Mỗi stack khai báo `key` riêng trong khối backend "azurerm" { key = "…" }.
# storage_account_name: lấy từ `terraform output tfstate_account` của bootstrap.
resource_group_name  = "rg-tfstate"
storage_account_name = "sttfstateXXXXX" # TODO(P1): điền sau khi apply bootstrap
container_name       = "tfstate"
use_azuread_auth     = true
