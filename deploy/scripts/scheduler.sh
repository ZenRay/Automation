#!/bin/bash
# Scheduler: runs cron_task.sh on a configurable interval
# Default interval: 3600 seconds (1 hour)

INTERVAL="${SCHEDULE_INTERVAL:-3600}"

echo "[scheduler] Starting with interval=${INTERVAL}s"

while true; do
    echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') Running cron_task.sh"
    bash /app/workers/cron_task.sh
    EXIT_CODE=$?
    echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') cron_task.sh exited with code ${EXIT_CODE}"
    echo "[scheduler] Sleeping ${INTERVAL}s..."
    sleep "$INTERVAL"
done
