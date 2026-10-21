variable "name" {
  type = string
}

variable "location" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "kubernetes_version" {
  description = "Minor version alias (AKS picks its latest GA patch)"
  type        = string
}

variable "node_size" {
  type = string
}

variable "node_count" {
  type    = number
  default = 1
}

variable "tags" {
  type    = map(string)
  default = {}
}
