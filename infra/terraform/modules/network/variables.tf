variable "name" {
  type = string
}

variable "location" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "address_space" {
  type    = string
  default = "10.10.0.0/16"
}

variable "admin_cidr" {
  description = "The only source allowed to reach SSH and the service ports, e.g. 203.0.113.7/32"
  type        = string
}

variable "service_ports" {
  type = list(number)
}

variable "tags" {
  type    = map(string)
  default = {}
}
