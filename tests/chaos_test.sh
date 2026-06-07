#!/bin/bash
# chaos_test.sh
# Injects latency and network partition using Toxiproxy sidecars.

set -e

TOXIPROXY_BASE=8470

# Helper: Apply toxic to a specific proxy on a specific toxiproxy instance
add_toxic() {
  local node_index=$1   # which node's toxiproxy (0-4)
  local proxy_name=$2   # proxy name inside that toxiproxy (e.g., "node1")
  local toxic_name=$3
  local toxic_type=$4
  local attributes=$5
  local api_port=$((TOXIPROXY_BASE + node_index))
  curl -s -X POST "http://localhost:${api_port}/proxies/${proxy_name}/toxics" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"${toxic_name}\", \"type\":\"${toxic_type}\", \"attributes\":${attributes}}"
  echo
}

remove_toxic() {
  local node_index=$1
  local proxy_name=$2
  local toxic_name=$3
  local api_port=$((TOXIPROXY_BASE + node_index))
  curl -s -X DELETE "http://localhost:${api_port}/proxies/${proxy_name}/toxics/${toxic_name}"
  echo
}

echo "=== Chaos test started ==="

# 1. Inject 2 seconds latency on traffic **to node1** (i.e., when any node sends to node1, via toxiproxy1)
# We target proxy "node1" inside toxiproxy1 (API port 8471)
add_toxic 1 "node1" "latency_to_node1" "latency" '{"latency":2000}'
echo "Injected 2s latency on all traffic to node1. Waiting 10 seconds..."
sleep 10

# Remove latency
remove_toxic 1 "node1" "latency_to_node1"
echo "Removed latency to node1."

# 2. Network partition: drop all traffic **to node3** (via toxiproxy3)
# Using bandwidth toxic with rate=0 effectively drops all packets.
add_toxic 3 "node3" "partition_node3" "bandwidth" '{"rate":0}'
echo "Partitioned node3 (all traffic to node3 is dropped). Waiting 15 seconds..."
sleep 15

# Remove partition
remove_toxic 3 "node3" "partition_node3"
echo "Recovered node3."

echo "=== Chaos test completed ==="