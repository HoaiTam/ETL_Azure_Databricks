variable "subscription_id_a" {
  description = "Sub A (Key Vault core). CI: TF_VAR_subscription_id_a."
  type        = string
}

variable "subscription_id_b" {
  description = "Sub B (Event Hubs). CI: TF_VAR_subscription_id_b."
  type        = string
}

variable "location_b" {
  description = "Region của Sub B, cố ý KHÁC region Sub A để đo độ trễ (rubric 3.3)."
  type        = string

  validation {
    condition     = length(var.location_b) > 0
    error_message = "Chưa chốt region Sub B ở G0 (infra/envs/20-stream.tfvars)."
  }
}

variable "tfstate_account" {
  description = "Storage account chứa state. CI: TF_VAR_tfstate_account."
  type        = string
}
