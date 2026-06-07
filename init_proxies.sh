#!/bin/bash
# init_proxies.sh
# Creates the necessary forwarding proxies inside each Toxiproxy sidecar.

set -e

TOXIPROXY_BASE=8470   # first Toxiproxy API port (toxiproxy0)

# List of node names and their actual container names (as seen inside Docker network)
# Node0's actual container name is "node0" (service name), listening on port 5000
# But inside the network, we can use the service name directly: node0:5000, node1:5001, etc.

# For each toxiproxy instance (0..4), create a proxy that listens on port 500X and forwards to the actual nodeX:500X.
for i in {0..4}; do
  API_PORT=$((TOXIPROXY_BASE + i))
  PROXY_PORT=$((5000 + i))
  NODE_NAME="node${i}"
  if [ $i -eq 4 ]; then
    NODE_NAME="adversary"   # node4 is the adversary service
  fi
  
  echo "Creating proxy for node${i} on toxiproxy${i} (API port ${API_PORT}) -> ${NODE_NAME}:${PROXY_PORT}"
  curl -s -X POST "http://localhost:${API_PORT}/proxies" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"${NODE_NAME}\", \"listen\":\":${PROXY_PORT}\", \"upstream\":\"${NODE_NAME}:${PROXY_PORT}\"}" \
    > /dev/null
  # Also create a proxy for the reverse direction? Not needed; each sidecar only needs to forward incoming connections to its own node.
done

echo "All proxies created successfully."