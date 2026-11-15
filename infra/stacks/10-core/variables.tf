variable "subscription_id_a" {
  description = "Subscription ID của Sub A. Local: terraform.auto.tfvars; CI: TF_VAR_subscription_id_a."
  type        = string
}

variable "location" {
  description = "Region chính đã chốt ở G0."
  type        = string

  validation {
    condition     = length(var.location) > 0
    error_message = "Chưa chốt region ở G0 (infra/envs/10-core.tfvars)."
  }
}
