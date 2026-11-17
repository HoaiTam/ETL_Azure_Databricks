# Stack 40-orchestration: linked service, dataset, pipeline, trigger của ADF.
# Xem docs/devops/06, mục 1. Apply SAU lần `bundle deploy -t prod` đầu tiên (cần job ID).
#
# [x] Remote state core + resource group core
# [ ] Linked service: ls_adls, ls_kv, ls_sql_retail, ls_zenodo, ls_off_api, ls_dbx (MSI)
# [ ] Dataset CSV / Parquet / JSON / SQL tham số hóa ingest_date
#     Instacart: ds_instacart_http_csv, ds_instacart_adls_csv, ds_instacart_adls_binary
#     (hợp đồng tham số và định nghĩa ở adf/datasets/; xem adf/README.md)
# [ ] Template 0 byte landing/ops/_SUCCESS từ adf/assets/_SUCCESS trước khi chạy ingest
# [ ] Pipeline pl_ingest_instacart_table (con) trước pl_ingest_instacart (cha)
#     Nạp pipeline_settings/<tên>.json: parameters String JSON + concurrency = 1
#     Cha depends_on pipeline con và các dataset, ExecutePipeline chờ hoàn tất
# [ ] Pipeline pl_ingest_retail_db, pl_ingest_off, pl_master
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
