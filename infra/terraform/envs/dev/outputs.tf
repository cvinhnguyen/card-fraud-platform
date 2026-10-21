output "vm_public_ip" {
  value = module.vm.public_ip
}

output "ssh" {
  value = "ssh ${var.admin_username}@${module.vm.public_ip}"
}

output "aks_name" {
  value = module.aks.name
}

output "resource_group" {
  value = azurerm_resource_group.this.name
}
