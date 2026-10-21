variable "env" {
  type    = string
  default = "dev"
}

variable "location" {
  # the student policy allows 5 EU regions; of them only polandcentral lets AKS use the B-series the quota covers
  description = "One of the regions the Azure for Students policy allows"
  type        = string
  default     = "polandcentral"
}

variable "admin_cidr" {
  description = "The admin's public IP as /32: the only source allowed to reach the VM"
  type        = string

  validation {
    condition     = can(cidrhost(var.admin_cidr, 0)) && var.admin_cidr != "0.0.0.0/0"
    error_message = "admin_cidr must be a CIDR and not the whole internet."
  }
}

variable "admin_username" {
  type    = string
  default = "azureuser"
}

variable "ssh_public_key_path" {
  type    = string
  default = "~/.ssh/id_ed25519.pub"
}

variable "vm_size" {
  type    = string
  default = "Standard_B2s_v2" # 2 vCPU, 8 GiB: builds and runs the model server image
}

variable "aks_node_size" {
  type    = string
  default = "Standard_B2s_v2"
}

variable "kubernetes_version" {
  type    = string
  default = "1.36"
}
