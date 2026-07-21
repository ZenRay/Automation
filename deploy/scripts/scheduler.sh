#!/usr/bin/env bash
# Scheduler: run cron_task.sh daily at a fixed time (Asia/Shanghai)

set -euo pipefail

export TZ='Asia/Shanghai'
SCHEDULE_HOUR="${SCHEDULE_HOUR:-7}"
SCHEDULE_MIN="${SCHEDULE_MIN:-30}"

# 逗号分隔的任务名，传递给 cron_task.sh --skip-task
# 默认跳过 cr_trail（数据量超飞书 5 万行限制，待修复后移除此默认值）
CRON_SKIP_TASKS="${CRON_SKIP_TASKS:-cr_trail}"

# 构建 --skip-task 参数数组
SKIP_ARGS=()
if [ -n "$CRON_SKIP_TASKS" ]; then
    IFS=',' read -ra _tasks <<< "$CRON_SKIP_TASKS"
    for _t in "${_tasks[@]}"; do
        SKIP_ARGS+=("--skip-task" "$_t")
    done
    echo "[scheduler] Skip tasks: $CRON_SKIP_TASKS"
fi

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
    if bash /app/workers/cron_task.sh "${SKIP_ARGS[@]+"${SKIP_ARGS[@]}"}"; then
        echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') cron_task.sh finished successfully"
    else
        exit_code=$?
        echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') cron_task.sh failed (exit_code=${exit_code})"
    fi

    # Avoid double trigger within the same minute.
    sleep 60
done
