variable "name" {
  description = "Tên workspace, ví dụ dbw-instacart-sbx-p4."
  type        = string
}

variable "resource_group_name" {
  description = "Resource group chứa workspace và access connector."
  type        = string
}

variable "location" {
  description = "Region. Cùng region với Sub A thì dùng chung được metastore Unity Catalog."
  type        = string
}

variable "sku" {
  description = "premium (cần cho Unity Catalog)."
  type        = string
  default     = "premium"
}

variable "tags" {
  description = "Tag từ module naming_tags."
  type        = map(string)
  default     = {}
}
