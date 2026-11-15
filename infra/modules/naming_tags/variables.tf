variable "project" {
  description = "Tên dự án, dùng trong tên tài nguyên và tag."
  type        = string
  default     = "instacart"
}

variable "env" {
  description = "prod | dev | sandbox"
  type        = string

  validation {
    condition     = contains(["prod", "dev", "sandbox"], var.env)
    error_message = "env phải là prod, dev hoặc sandbox."
  }
}

variable "owner" {
  description = "Người chịu trách nhiệm tài nguyên: p1 | p2 | p3 | p4."
  type        = string
}

variable "stack" {
  description = "Tên stack, ví dụ 10-core."
  type        = string
}

variable "extra_tags" {
  description = "Tag bổ sung."
  type        = map(string)
  default     = {}
}
