# AKS (Free tier: no control plane fee) with one system node pool, Azure CNI overlay, and a managed identity.
resource "azurerm_kubernetes_cluster" "this" {
  name                = var.name
  location            = var.location
  resource_group_name = var.resource_group_name
  dns_prefix          = var.name
  kubernetes_version  = var.kubernetes_version
  sku_tier            = "Free"
  tags                = var.tags

  default_node_pool {
    name            = "system"
    vm_size         = var.node_size
    node_count      = var.node_count
    os_disk_size_gb = 30

    upgrade_settings {
      max_surge = "10%"
    }
  }

  identity {
    type = "SystemAssigned"
  }

  network_profile {
    network_plugin      = "azure"
    network_plugin_mode = "overlay"
  }

  node_provisioning_profile {
    mode = "Manual" # node pools are declared here, not auto-provisioned
  }
}
