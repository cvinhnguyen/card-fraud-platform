# Container registry for the images AKS runs (Basic: the cheapest tier). No admin user: AKS pulls with its kubelet
# identity (AcrPull, granted in the environment), the admin pushes after `az acr login`.
resource "azurerm_container_registry" "this" {
  name                = var.name
  location            = var.location
  resource_group_name = var.resource_group_name
  sku                 = "Basic"
  admin_enabled       = false
  tags                = var.tags
}
