output "vm_subnet_id" {
  # the association, so a VM in this subnet is only created once the NSG protects it
  value = azurerm_subnet_network_security_group_association.vm.subnet_id
}
