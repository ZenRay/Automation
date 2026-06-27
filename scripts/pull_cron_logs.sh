#!/usr/bin/env bash
# Pull cron/persistence logs from remote host to local machine.
# This script is local-only tooling and is not used in container runtime.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  scripts/pull_cron_logs.sh --host <user@host> [options]

Options:
  --host <user@host>            Remote SSH target (required)
  --remote-dir <path>           Remote project root (default: /opt/automation)
  --local-dir <path>            Local output dir (default: ./logs/remote_pull)
  --days <n>                    Pull latest n days by mtime filter (default: 4)
  --ssh-port <port>             SSH port (default: 22)
  --method <rsync|scp>          Transfer method (default: rsync, fallback to scp)

Examples:
  scripts/pull_cron_logs.sh --host ray@example.com
  scripts/pull_cron_logs.sh --host ray@example.com --remote-dir /srv/automation --days 7
EOF
}

HOST=""
REMOTE_DIR="/opt/automation"
LOCAL_DIR="./logs/remote_pull"
DAYS="4"
SSH_PORT="22"
METHOD="rsync"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --host) HOST="$2"; shift 2 ;;
        --remote-dir) REMOTE_DIR="$2"; shift 2 ;;
        --local-dir) LOCAL_DIR="$2"; shift 2 ;;
        --days) DAYS="$2"; shift 2 ;;
        --ssh-port) SSH_PORT="$2"; shift 2 ;;
        --method) METHOD="$2"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown option: $1"; usage; exit 2 ;;
    esac
done

if [[ -z "$HOST" ]]; then
    echo "Error: --host is required"
    usage
    exit 2
fi

TS="$(date '+%Y%m%d_%H%M%S')"
TARGET_DIR="$LOCAL_DIR/$TS"
mkdir -p "$TARGET_DIR"

REMOTE_PERSISTENCE="$REMOTE_DIR/logs/persistence"

echo "[pull] host=$HOST"
echo "[pull] remote=$REMOTE_PERSISTENCE"
echo "[pull] local=$TARGET_DIR"

if [[ "$METHOD" == "rsync" ]] && command -v rsync >/dev/null 2>&1; then
    RSYNC_SSH="ssh -p $SSH_PORT"
    rsync -avz --prune-empty-dirs \
        --include='*/' \
        --include='cron/***' \
        --include='upgrade_after_sale/***' \
        --exclude='*' \
        -e "$RSYNC_SSH" \
        "$HOST:$REMOTE_PERSISTENCE/" "$TARGET_DIR/"
else
    echo "[pull] rsync unavailable or disabled, fallback to scp"
    scp -P "$SSH_PORT" -r "$HOST:$REMOTE_PERSISTENCE/cron" "$TARGET_DIR/" 2>/dev/null || true
    scp -P "$SSH_PORT" -r "$HOST:$REMOTE_PERSISTENCE/upgrade_after_sale" "$TARGET_DIR/" 2>/dev/null || true
fi

if [[ -d "$TARGET_DIR/upgrade_after_sale" ]]; then
    # Keep recent days only in local pulled archive for convenience.
    find "$TARGET_DIR/upgrade_after_sale" -mindepth 1 -maxdepth 1 -type d -mtime "+$DAYS" -exec rm -rf {} +
fi

echo "[pull] done: $TARGET_DIR"
