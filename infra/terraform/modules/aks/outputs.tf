output "name" {
  value = azurerm_kubernetes_cluster.this.name
}

output "kubelet_object_id" {
  # the identity the nodes pull images with
  value = azurerm_kubernetes_cluster.this.kubelet_identity[0].object_id
}

output "kube_config_raw" {
  value     = azurerm_kubernetes_cluster.this.kube_config_raw
  sensitive = true
}
