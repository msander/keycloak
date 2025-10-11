#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.valkey.yml"
RUNTIME_DIR="${SCRIPT_DIR}/runtime"
PROVIDERS_DIR="${RUNTIME_DIR}/providers"
PROJECT_NAME="keycloak-valkey"
KEYCLOAK_SERVICES=(keycloak-primary keycloak-secondary)

usage() {
    cat <<USAGE
Usage: ${0##*/} [command]

Commands:
  up        Build the Valkey module (unless SKIP_BUILD=1) and start the stack in the foreground (default).
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
    echo "Starting Valkey + dual Keycloak stack in the foreground. Press Ctrl+C to stop."

    set +e
    "${COMPOSE_CMD[@]}" up --abort-on-container-exit --remove-orphans
    local up_status=$?
    set -e

    local exit_code=$up_status
    for service in "${KEYCLOAK_SERVICES[@]}"; do
        local container_id
        container_id=$("${COMPOSE_CMD[@]}" ps -q "${service}" 2>/dev/null || true)
        if [[ -n "${container_id}" ]]; then
            local service_exit
            service_exit=$(docker inspect -f '{{.State.ExitCode}}' "${container_id}" 2>/dev/null || echo 0)
            if [[ "${service_exit}" != "0" ]]; then
                echo "Keycloak service '${service}' exited with status ${service_exit}; shutting down the stack." >&2
                exit_code=${service_exit}
                break
            fi
        fi
    done

    set +e
    "${COMPOSE_CMD[@]}" down --remove-orphans >/dev/null 2>&1
    set -e

    exit "${exit_code}"
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
