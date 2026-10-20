#!/bin/sh
# External routing (after observability.sh): F5 NGINX Ingress Controller 5.6.3 (chart 2.7.3) on the kind node's
# ports 80/443 (kind.yaml maps them to 127.0.0.1), then the edge chart: one Ingress per UI / API with basic auth and
# rate limits. Idempotent.
#   domain  DUCKDNS_DOMAIN from infra/.env (e.g. cardfraud-vin.duckdns.org, pointed at 127.0.0.1), else localtest.me
#   https   on once the wildcard certificate secret nginx-ingress/wildcard-tls exists (tls.sh)
# Open: https://agents.<domain> (and registry, grafana, logs, traces, api) - user admin, EDGE_ADMIN_PASSWORD
set -e
cd "$(dirname "$0")"
NIC=2.7.3
DOMAIN=$(grep "^DUCKDNS_DOMAIN=" ../.env 2>/dev/null | cut -d= -f2-)
DOMAIN=${DOMAIN:-localtest.me}
TLS=false; set --
if kubectl -n nginx-ingress get secret wildcard-tls >/dev/null 2>&1; then
  TLS=true; set -- --set controller.wildcardTLS.secret=nginx-ingress/wildcard-tls
fi
helm upgrade -i nginx-ingress oci://ghcr.io/nginx/charts/nginx-ingress -n nginx-ingress --create-namespace \
  --version "$NIC" --set controller.hostPort.enable=true --set controller.service.type=ClusterIP \
  --set controller.strategy.type=Recreate `# one node, ports 80/443 on the host: a rolling update could not schedule` \
  --set controller.ingressClass.setAsDefaultIngress=true \
  --set controller.resources.requests.memory=64Mi --set controller.resources.limits.memory=256Mi "$@" --wait
sh secrets.sh   # edge-basic-auth in each namespace with a protected route
helm upgrade -i edge edge -n nginx-ingress --set domain="$DOMAIN" --set tls="$TLS"
echo "edge ready: *.$DOMAIN (tls=$TLS)"
