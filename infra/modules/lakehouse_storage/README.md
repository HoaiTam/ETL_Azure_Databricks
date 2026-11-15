# Module `lakehouse_storage` (chưa viết)

Wrapper quanh `Azure/avm-res-storage-storageaccount/azurerm`: ADLS Gen2 (HNS), các container theo tầng
(`landing`, `bronze`, `silver`, `gold`, `checkpoints`, `mlflow`) và lifecycle `landing/` → cool sau 30 ngày.

Viết ở bước 3.2 của stack `10-core` ([docs/devops/03](../../../docs/devops/03_terraform_azure.md)).
Trước khi viết: pin `version` chính xác của module AVM và kiểm tra tên tham số trong README của module.
