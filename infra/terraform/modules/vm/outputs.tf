output "name" {
  value = azurerm_linux_virtual_machine.this.name
}

output "public_ip" {
  value = azurerm_public_ip.this.ip_address
}
