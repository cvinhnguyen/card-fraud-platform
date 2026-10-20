#!/bin/sh
# HTTPS for every edge host, the Web API included: one Let's Encrypt wildcard certificate *.<DUCKDNS_DOMAIN> by
# DNS-01 through DuckDNS (this machine is not reachable from the internet, so HTTP-01 cannot work), obtained with lego
# in a container. Cert + key go into Vault (secrets.sh); ESO syncs them to nginx-ingress/wildcard-tls, NGINX's
# wildcard TLS secret; edge.sh then turns on TLS and the HTTP -> HTTPS redirect. Renew: run again within 30 days of
# expiry (certificates last 90 days).
# Needs in infra/.env: DUCKDNS_DOMAIN (e.g. cardfraud-vin.duckdns.org, its IP set to 127.0.0.1 on duckdns.org) and
# DUCKDNS_TOKEN; optional LETSENCRYPT_EMAIL. Certificate files stay in infra/.certs/ (gitignored).
set -e
cd "$(dirname "$0")/../.."
LEGO=v5.5.2
val() { grep "^$1=" infra/.env | cut -d= -f2-; }
DOMAIN=$(val DUCKDNS_DOMAIN)
[ -n "$DOMAIN" ] && [ -n "$(val DUCKDNS_TOKEN)" ] || { echo "set DUCKDNS_DOMAIN and DUCKDNS_TOKEN in infra/.env" >&2; exit 1; }
# point the domain (and so every *.domain) at this machine; curl reads the URL with the token from stdin
printf 'url = "https://www.duckdns.org/update?domains=%s&token=%s&ip=127.0.0.1"\n' "${DOMAIN%%.duckdns.org}" \
  "$(val DUCKDNS_TOKEN)" | curl -fsS -K - | grep -qx OK || { echo "DuckDNS update failed (token / domain?)" >&2; exit 1; }
echo "DuckDNS: $DOMAIN -> 127.0.0.1"
mkdir -p infra/.certs
dir=$(cd infra/.certs && (pwd -W 2>/dev/null || pwd))   # Windows path under Git Bash, plain path elsewhere
email=$(val LETSENCRYPT_EMAIL || true)
# the token reaches the container through the environment (-e NAME without a value), not the command line
DUCKDNS_TOKEN=$(val DUCKDNS_TOKEN) MSYS_NO_PATHCONV=1 docker run --rm -e DUCKDNS_TOKEN -v "$dir:/data" \
  "goacme/lego:$LEGO" run --accept-tos --dns duckdns -d "*.$DOMAIN" --key-type ec256 --path /data ${email:+--email "$email"}
sh infra/k8s/secrets.sh   # cert + key -> Vault -> nginx-ingress/wildcard-tls
sh infra/k8s/edge.sh      # wildcard TLS on, HTTPS hosts
