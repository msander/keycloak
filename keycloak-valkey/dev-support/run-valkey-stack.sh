#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.valkey.yml"
RUNTIME_DIR="${SCRIPT_DIR}/runtime"
PROVIDERS_DIR="${RUNTIME_DIR}/providers"
PROJECT_NAME="keycloak-valkey"
KEYCLOAK_SERVICES=(keycloak-primary keycloak-secondary)
STACK_STARTED=0
LOGS_PID=0
HTTP_CLIENT=""

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

cleanup_stack() {
    local status=$?
    trap - EXIT

    if [[ "${LOGS_PID}" -gt 0 ]]; then
        kill "${LOGS_PID}" >/dev/null 2>&1 || true
        wait "${LOGS_PID}" 2>/dev/null || true
    fi

    if [[ "${STACK_STARTED}" -eq 1 ]]; then
        set +e
        "${COMPOSE_CMD[@]}" down --remove-orphans >/dev/null 2>&1 || true
        set -e
    fi

    exit "${status}"
}

handle_interrupt() {
    echo "Interrupt received. Tearing down the Valkey stack..."
    exit 130
}

ensure_http_client() {
    if [[ -n "${HTTP_CLIENT}" ]]; then
        return
    fi
    if command -v curl >/dev/null 2>&1; then
        HTTP_CLIENT="curl"
    elif command -v wget >/dev/null 2>&1; then
        HTTP_CLIENT="wget"
    else
        echo "Either 'curl' or 'wget' is required to check Keycloak readiness." >&2
        exit 1
    fi
}

http_probe() {
    local url="$1"
    case "${HTTP_CLIENT}" in
        curl)
            curl --silent --fail --max-time 5 "${url}" >/dev/null 2>&1
            ;;
        wget)
            wget -q -O- --timeout=5 "${url}" >/dev/null 2>&1
            ;;
        *)
            return 1
            ;;
    esac
}

keycloak_service_port() {
    case "$1" in
        keycloak-primary)
            echo 8080
            ;;
        keycloak-secondary)
            echo 8081
            ;;
        *)
            echo "" && return 1
            ;;
    esac
}

wait_for_service_health() {
    local service="$1"
    local timeout="${2:-180}"
    local start_time
    start_time=$(date +%s)

    while true; do
        local container_id
        container_id=$("${COMPOSE_CMD[@]}" ps -q "${service}" 2>/dev/null || true)
        if [[ -n "${container_id}" ]]; then
            local health
            health=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "${container_id}" 2>/dev/null || echo "")
            if [[ -z "${health}" ]]; then
                echo "Service '${service}' has no health check; assuming it is ready."
                return 0
            fi
            if [[ "${health}" == "healthy" ]]; then
                echo "Service '${service}' is healthy."
                return 0
            fi
            if [[ "${health}" == "unhealthy" ]]; then
                echo "Service '${service}' reported an unhealthy state." >&2
                return 1
            fi
        fi
        local now
        now=$(date +%s)
        if (( now - start_time >= timeout )); then
            echo "Timed out waiting for service '${service}' to become healthy." >&2
            return 1
        fi
        sleep 2
    done
}

wait_for_keycloak_ready() {
    local service="$1"
    local timeout="${2:-240}"
    local port
    port=$(keycloak_service_port "${service}") || return 1

    ensure_http_client

    local start_time
    start_time=$(date +%s)

    while true; do
        local container_id
        container_id=$("${COMPOSE_CMD[@]}" ps -q "${service}" 2>/dev/null || true)
        if [[ -z "${container_id}" ]]; then
            echo "Waiting for Keycloak service '${service}' container to start..."
            sleep 2
            continue
        fi

        local state
        state=$(docker inspect -f '{{.State.Status}}' "${container_id}" 2>/dev/null || echo "")
        if [[ "${state}" == "exited" || "${state}" == "dead" ]]; then
            local exit_code
            exit_code=$(docker inspect -f '{{.State.ExitCode}}' "${container_id}" 2>/dev/null || echo 1)
            echo "Keycloak service '${service}' exited with status ${exit_code} before becoming ready." >&2
            return ${exit_code}
        fi

        if http_probe "http://localhost:${port}/health/ready"; then
            echo "Keycloak service '${service}' is ready at http://localhost:${port}."
            return 0
        fi

        local now
        now=$(date +%s)
        if (( now - start_time >= timeout )); then
            echo "Timed out waiting for Keycloak service '${service}' to report ready." >&2
            return 1
        fi
        sleep 3
    done
}

monitor_keycloak_services() {
    local exit_code=0
    while true; do
        for service in "$@"; do
            local container_id
            container_id=$("${COMPOSE_CMD[@]}" ps -q "${service}" 2>/dev/null || true)
            [[ -z "${container_id}" ]] && continue

            local state
            state=$(docker inspect -f '{{.State.Status}}' "${container_id}" 2>/dev/null || echo "")
            if [[ "${state}" == "exited" || "${state}" == "dead" ]]; then
                local service_exit
                service_exit=$(docker inspect -f '{{.State.ExitCode}}' "${container_id}" 2>/dev/null || echo 0)
                if [[ "${service_exit}" != "0" ]]; then
                    echo "Keycloak service '${service}' exited with status ${service_exit}; shutting down the stack." >&2
                    exit_code=${service_exit}
                else
                    echo "Keycloak service '${service}' exited cleanly; shutting down the stack."
                    exit_code=0
                fi
                return ${exit_code}
            fi
        done
        sleep 2
    done
}

cmd_up() {
    build_module
    stage_provider
    ensure_compose_available
    trap cleanup_stack EXIT
    trap handle_interrupt INT TERM

    echo "Preparing Valkey development stack..."

    set +e
    "${COMPOSE_CMD[@]}" down --remove-orphans >/dev/null 2>&1
    set -e

    echo "Starting infrastructure services (Valkey, Postgres)..."
    "${COMPOSE_CMD[@]}" up --detach --remove-orphans valkey postgres
    STACK_STARTED=1

    wait_for_service_health valkey
    wait_for_service_health postgres

    for service in "${KEYCLOAK_SERVICES[@]}"; do
        echo "Starting Keycloak service '${service}'..."
        "${COMPOSE_CMD[@]}" up --detach "${service}"
        wait_for_keycloak_ready "${service}"
    done

    echo "All services are running. Attaching to stack logs (Ctrl+C to stop)..."
    "${COMPOSE_CMD[@]}" logs -f &
    LOGS_PID=$!

    monitor_keycloak_services "${KEYCLOAK_SERVICES[@]}"
    local exit_code=$?
    return ${exit_code}
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
