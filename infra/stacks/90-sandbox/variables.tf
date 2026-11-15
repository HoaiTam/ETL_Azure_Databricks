variable "subscription_id_sandbox" {
  description = "Sub C hoặc Sub D."
  type        = string
}

variable "member" {
  description = "Người sở hữu sandbox: p1 | p2 | p3 | p4."
  type        = string
}

variable "location_c" {
  description = "Nên CÙNG region với Sub A để dùng chung metastore Unity Catalog."
  type        = string

  validation {
    condition     = length(var.location_c) > 0
    error_message = "Chưa chốt region sandbox (infra/envs/90-sandbox-<member>.tfvars)."
  }
}
