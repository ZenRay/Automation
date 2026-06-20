#!/bin/bash
# Deployment script for Automation container stack
# Usage: deploy.sh <command> [options]
# Commands: deploy, rollback, status, list-tags

set -euo pipefail

COMPOSE_BASE="deploy/compose.yml"
COMPOSE_DIR="$(dirname "$(readlink -f "$0")")/.."

usage() {
    echo "Usage: $0 <command> [options]"
    echo ""
    echo "Commands:"
    echo "  deploy   --env <staging|production> [--tag <image-tag>]"
    echo "  rollback --env <staging|production> --tag <image-tag>"
    echo "  status   --env <staging|production>"
    echo "  list-tags [--limit <n>]"
    exit 2
}

parse_env() {
    if [ -z "${ENV:-}" ]; then
        echo "Error: --env is required"
        usage
    fi
    if [[ "$ENV" != "staging" && "$ENV" != "production" ]]; then
        echo "Error: --env must be 'staging' or 'production'"
        exit 1
    fi
    COMPOSE_OVERRIDE="${COMPOSE_DIR}/compose.${ENV}.yml"
    if [ ! -f "$COMPOSE_OVERRIDE" ]; then
        echo "Error: Override file not found: $COMPOSE_OVERRIDE"
        exit 1
    fi
}

cmd_deploy() {
    ENV=""
    TAG=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --env) ENV="$2"; shift 2 ;;
            --tag) TAG="$2"; shift 2 ;;
            *) echo "Unknown option: $1"; usage ;;
        esac
    done
    parse_env

    if [ -n "$TAG" ]; then
        export IMAGE_TAG="$TAG"
    else
        export IMAGE_TAG="$ENV"
    fi

    echo "Deploying to ${ENV} with tag ${IMAGE_TAG}..."
    export GHCR_REPO="${GHCR_REPO:-automation}"
    export CONFIG_DIR="${CONFIG_DIR:-/opt/automation/config}"

    podman compose -f "${COMPOSE_DIR}/compose.yml" -f "$COMPOSE_OVERRIDE" pull
    podman compose -f "${COMPOSE_DIR}/compose.yml" -f "$COMPOSE_OVERRIDE" up -d

    echo "Waiting for health checks (60s timeout)..."
    TIMEOUT=60
    ELAPSED=0
    while [ $ELAPSED -lt $TIMEOUT ]; do
        STATUS=$(podman compose -f "${COMPOSE_DIR}/compose.yml" -f "$COMPOSE_OVERRIDE" ps --format '{{.Health}}' 2>/dev/null || echo "unknown")
        if echo "$STATUS" | grep -q "healthy"; then
            echo "Deployment successful: all services healthy"
            return 0
        fi
        sleep 5
        ELAPSED=$((ELAPSED + 5))
    done
    echo "Warning: Health check timeout after ${TIMEOUT}s"
    podman compose -f "${COMPOSE_DIR}/compose.yml" -f "$COMPOSE_OVERRIDE" ps
    return 1
}

cmd_rollback() {
    ENV=""
    TAG=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --env) ENV="$2"; shift 2 ;;
            --tag) TAG="$2"; shift 2 ;;
            *) echo "Unknown option: $1"; usage ;;
        esac
    done
    parse_env

    if [ -z "$TAG" ]; then
        echo "Error: --tag is required for rollback"
        exit 1
    fi

    echo "Rolling back ${ENV} to tag ${TAG}..."
    export IMAGE_TAG="$TAG"
    export GHCR_REPO="${GHCR_REPO:-automation}"
    export CONFIG_DIR="${CONFIG_DIR:-/opt/automation/config}"

    podman compose -f "${COMPOSE_DIR}/compose.yml" -f "$COMPOSE_OVERRIDE" pull
    podman compose -f "${COMPOSE_DIR}/compose.yml" -f "$COMPOSE_OVERRIDE" up -d

    echo "Rollback complete. Current status:"
    cmd_status --env "$ENV"
}

cmd_status() {
    ENV=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --env) ENV="$2"; shift 2 ;;
            *) echo "Unknown option: $1"; usage ;;
        esac
    done
    parse_env

    echo "Environment: ${ENV}"
    podman compose -f "${COMPOSE_DIR}/compose.yml" -f "$COMPOSE_OVERRIDE" ps --format "table {{.Name}}\t{{.Status}}\t{{.Image}}"
}

cmd_list_tags() {
    LIMIT=10
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --limit) LIMIT="$2"; shift 2 ;;
            *) echo "Unknown option: $1"; usage ;;
        esac
    done

    echo "Recent image tags (limit: ${LIMIT}):"
    podman images --format "{{.Repository}}:{{.Tag}}\t{{.CreatedAt}}" localhost/automation | head -n "$LIMIT"
}

# Main dispatch
if [ $# -lt 1 ]; then
    usage
fi

COMMAND="$1"
shift

case "$COMMAND" in
    deploy)    cmd_deploy "$@" ;;
    rollback)  cmd_rollback "$@" ;;
    status)    cmd_status "$@" ;;
    list-tags) cmd_list_tags "$@" ;;
    *)         echo "Unknown command: $COMMAND"; usage ;;
esac
