variable "subscription_id_a" {
  description = "Subscription ID của Sub A. CI: TF_VAR_subscription_id_a."
  type        = string
}

variable "tfstate_account" {
  description = "Storage account chứa state. CI: TF_VAR_tfstate_account."
  type        = string
}
