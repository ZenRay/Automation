#!/usr/bin/env bash
# Scheduler: run cron_task.sh daily at a fixed time (Asia/Shanghai)

set -euo pipefail

export TZ='Asia/Shanghai'
SCHEDULE_HOUR="${SCHEDULE_HOUR:-7}"
SCHEDULE_MIN="${SCHEDULE_MIN:-30}"

echo "[scheduler] Starting, schedule: daily at ${SCHEDULE_HOUR}:$(printf '%02d' "$SCHEDULE_MIN")"

while true; do
    now_h=$(date '+%H')
    now_m=$(date '+%M')
    now_total=$((10#$now_h * 60 + 10#$now_m))
    target_total=$((10#$SCHEDULE_HOUR * 60 + 10#$SCHEDULE_MIN))

    if [[ "$now_total" -lt "$target_total" ]]; then
        wait_min=$((target_total - now_total))
    else
        wait_min=$((1440 - now_total + target_total))
    fi

    echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') Sleeping ${wait_min} minutes until next run"
    sleep "$((wait_min * 60))"

    echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') Running cron_task.sh"
    if bash /app/workers/cron_task.sh; then
        echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') cron_task.sh finished successfully"
    else
        exit_code=$?
        echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') cron_task.sh failed (exit_code=${exit_code})"
    fi

    # Avoid double trigger within the same minute.
    sleep 60
done
