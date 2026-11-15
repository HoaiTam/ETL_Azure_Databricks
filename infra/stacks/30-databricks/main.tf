# Stack 30-databricks: bên trong workspace (Unity Catalog, quyền, policy, warehouse).
# Xem docs/devops/05. Cần 10-core đã có workspace (bước 3.5) trước khi plan.
#
# [x] 1. Xác nhận metastore
# [ ] 2. Service principal cho CI (application_id = ci_client_id), user theo vai trò
# [ ] 3. Storage credential + external location theo container
# [ ] 4. Catalog instacart, 7 schema, volume raw + checkpoints
# [ ] 5. Grants Unity Catalog (một khối databricks_grants cho mỗi securable)
# [ ] 6. Cluster policy single-node + tắt unrestricted cluster creation
# [ ] 7. Secret scope dựa trên Key Vault (P1 apply từ máy local)
# [ ] 8. Serverless SQL warehouse wh-instacart-bi
# [ ] 9. Output cho DAB và Power BI

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

locals {
  core = data.terraform_remote_state.core.outputs
}

# Lỗi ở đây nghĩa là workspace chưa được gán metastore → đi nhánh dự phòng của spike 3.4.
data "databricks_current_metastore" "this" {}
