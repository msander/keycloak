#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.valkey.yml"
RUNTIME_DIR="${SCRIPT_DIR}/runtime"
PROVIDERS_DIR="${RUNTIME_DIR}/providers"
PROJECT_NAME="keycloak-valkey"

usage() {
    cat <<USAGE
Usage: ${0##*/} [command]

Commands:
  up        Build the Valkey module (unless SKIP_BUILD=1) and start the stack in the background (default).
  down      Stop the stack and remove containers.
  logs      Tail the stack logs.
  ps        Show container status.

Environment variables:
  SKIP_BUILD=1         Skip building the Valkey module before copying the provider JAR.
  STANDALONE_BUILD=1   Build against the standalone POM instead of the repository aggregator (requires upstream Keycloak deps installed locally).

USAGE
}

ensure_compose_available() {
    if ! command -v docker >/dev/null 2>&1; then
        echo "Docker CLI is required to manage the test stack." >&2
        exit 1
    fi
    if docker compose version >/dev/null 2>&1; then
        COMPOSE_CMD=(docker compose --project-name "${PROJECT_NAME}" -f "${COMPOSE_FILE}")
    elif command -v docker-compose >/dev/null 2>&1; then
        COMPOSE_CMD=(docker-compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}")
    else
        echo "Docker Compose v2 or v1 is required to manage the test stack." >&2
        exit 1
    fi
}

build_module() {
    if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
        return
    fi
    echo "Building Keycloak Valkey module..."
    if [[ "${STANDALONE_BUILD:-0}" == "1" ]]; then
        mvn -f "${MODULE_DIR}/pom-standalone.xml" -DskipTests package
    else
        mvn -f "${MODULE_DIR}/../pom.xml" -pl keycloak-valkey -am -DskipTests package
    fi
}

stage_provider() {
    mkdir -p "${PROVIDERS_DIR}"
    local jar
    jar=$(ls -1t "${MODULE_DIR}/target"/keycloak-valkey-*.jar 2>/dev/null | head -n1 || true)
    if [[ -z "${jar}" ]]; then
        echo "No Keycloak Valkey provider JAR found. Run 'mvn -f ../pom.xml -pl keycloak-valkey -am -DskipTests package' first or adjust the build settings." >&2
        exit 1
    fi
    echo "Copying ${jar##*/} into runtime providers directory..."
    cp "${jar}" "${PROVIDERS_DIR}/"
}

cmd_up() {
    build_module
    stage_provider
    ensure_compose_available
    "${COMPOSE_CMD[@]}" up -d --remove-orphans
    echo "Valkey + dual Keycloak stack is running. Use '${0##*/} logs' to follow logs."
}

cmd_down() {
    ensure_compose_available
    "${COMPOSE_CMD[@]}" down
}

cmd_logs() {
    ensure_compose_available
    "${COMPOSE_CMD[@]}" logs -f
}

cmd_ps() {
    ensure_compose_available
    "${COMPOSE_CMD[@]}" ps
}

COMMAND="${1:-up}"
case "${COMMAND}" in
    up)
        cmd_up
        ;;
    down)
        cmd_down
        ;;
    logs)
        cmd_logs
        ;;
    ps)
        cmd_ps
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        echo "Unknown command: ${COMMAND}" >&2
        usage
        exit 1
        ;;

esac
