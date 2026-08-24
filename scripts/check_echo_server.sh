#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

compose_file="${COMPOSE_FILE:-docker-compose.yaml}"
server_service="${SERVER_SERVICE:-server}"
server_port="${SERVER_PORT:-5678}"
netcat_image="${NETCAT_IMAGE:-busybox:latest}"
message="${1:-Hello World}"
project_name="${COMPOSE_PROJECT_NAME:-$(basename "$repo_root")}"
server_image="${SERVER_IMAGE:-$project_name-$server_service:latest}"
run_id="echo-check-$RANDOM-$$"
network="$run_id-network"
server_container="$run_id-server"

cleanup() {
    docker rm -f "$server_container" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose -f "$compose_file" build "$server_service" >/dev/null

if ! docker image inspect "$server_image" >/dev/null 2>&1; then
    echo "Could not find image '$server_image' for service '$server_service'" >&2
    exit 1
fi

docker network create "$network" >/dev/null
docker run \
    --detach \
    --name "$server_container" \
    --network "$network" \
    --network-alias "$server_service" \
    --env PYTHONUNBUFFERED=1 \
    --env SERVER_HOST=0.0.0.0 \
    --env SERVER_PORT="$server_port" \
    "$server_image" >/dev/null

sleep 1

response="$(
    docker run --rm --network "$network" "$netcat_image" sh -c '
        printf "%s\n" "$1" | timeout 5 nc -w 1 "$2" "$3"
    ' sh "$message" "$server_service" "$server_port"
)"

if [[ "$response" != "$message" ]]; then
    echo "Echo server check failed" >&2
    echo "Expected: $message" >&2
    echo "Received: $response" >&2
    exit 1
fi

echo "Echo server check succeeded: $response"
