# Stack 40-orchestration: linked service, dataset, pipeline, trigger của ADF.
# Xem docs/devops/06, mục 1. Apply SAU lần `bundle deploy -t prod` đầu tiên (cần job ID).
#
# [x] Remote state core + resource group core
# [ ] Linked service: ls_adls, ls_kv, ls_sql_retail, ls_zenodo, ls_off_api, ls_dbx (MSI)
# [ ] Dataset CSV / Parquet / JSON / SQL tham số hóa ingest_date
# [ ] Pipeline pl_ingest_instacart, pl_ingest_retail_db, pl_ingest_off, pl_master
#     (activities_json = file("${path.module}/../../../adf/pipelines/<tên>.json"))
# [ ] Trigger schedule, activated = var.triggers_on (mặc định false)

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

data "azurerm_resource_group" "core" {
  name = data.terraform_remote_state.core.outputs.rg_name
}
