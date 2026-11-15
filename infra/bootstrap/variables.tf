variable "subscription_id_a" {
  description = "Subscription ID của Sub A (core). Đặt trong bootstrap.auto.tfvars, không commit."
  type        = string
}

variable "location" {
  description = "Region chính đã chốt ở G0 (docs/devops/spike_results.md)."
  type        = string

  validation {
    condition     = length(var.location) > 0
    error_message = "Chưa chốt region ở G0."
  }
}

variable "github_repo" {
  description = "Repo GitHub dạng <owner>/<repo>, dùng làm subject cho federated credential."
  type        = string

  validation {
    condition     = can(regex("^[^/]+/[^/]+$", var.github_repo))
    error_message = "github_repo phải có dạng <owner>/<repo>."
  }
}

variable "member_object_ids" {
  description = "Map tên vai → Entra Object ID của 4 thành viên, ví dụ { p1 = \"…\", p2 = \"…\" }."
  type        = map(string)
}
