output "tags" {
  description = "Tag chuẩn: project, env, owner, stack, managed_by (+ extra_tags)."
  value       = local.tags
}

output "names" {
  description = "Toàn bộ output của Azure/naming/azurerm, ví dụ names.storage_account.name_unique."
  value       = module.naming
}
