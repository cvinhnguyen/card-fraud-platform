# dev environment of the platform on Azure: a resource group, the network, the model-serving VM that Ansible
# configures (infra/ansible) and an AKS cluster. Sized for an Azure for Students subscription (6 vCPU per region,
# regions limited by policy): 2 vCPU for the VM + 2 for the AKS node.
#
#   az login && export ARM_SUBSCRIPTION_ID=$(az account show --query id -o tsv)
#   terraform init
#   terraform apply -var admin_cidr=$(curl -s https://api.ipify.org)/32
#   kubectl --kubeconfig ../../../.secrets/aks-kubeconfig get nodes
#   terraform destroy -var admin_cidr=0.0.0.0/32      # when done: the VM and the node cost by the hour
#
# State is local (gitignored). ponytail: one person runs this; move it to an azurerm backend when a team shares it.
locals {
  name = "cardfraud-${var.env}"
  tags = { project = "card-fraud-platform", env = var.env, managed_by = "terraform" }
}

resource "azurerm_resource_group" "this" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = local.tags
}

module "network" {
  source              = "../../modules/network"
  name                = local.name
  location            = var.location
  resource_group_name = azurerm_resource_group.this.name
  admin_cidr          = var.admin_cidr
  service_ports       = [8080, 9100] # model server, node_exporter
  tags                = local.tags
}

module "vm" {
  source              = "../../modules/vm"
  name                = "${local.name}-serving"
  location            = var.location
  resource_group_name = azurerm_resource_group.this.name
  subnet_id           = module.network.vm_subnet_id
  size                = var.vm_size
  admin_username      = var.admin_username
  ssh_public_key      = file(pathexpand(var.ssh_public_key_path))
  tags                = local.tags
}

module "aks" {
  source              = "../../modules/aks"
  name                = "aks-${local.name}"
  location            = var.location
  resource_group_name = azurerm_resource_group.this.name
  kubernetes_version  = var.kubernetes_version
  node_size           = var.aks_node_size
  tags                = local.tags
}

# Ansible's inventory (gitignored: it holds the VM's public IP)
resource "local_file" "inventory" {
  filename        = "${path.module}/../../../ansible/inventory/hosts.ini"
  file_permission = "0644"
  content         = <<-EOT
    [serving]
    ${module.vm.name} ansible_host=${module.vm.public_ip} ansible_user=${var.admin_username}
  EOT
}

resource "local_sensitive_file" "kubeconfig" {
  filename        = "${path.module}/../../../.secrets/aks-kubeconfig"
  file_permission = "0600"
  content         = module.aks.kube_config_raw
}
