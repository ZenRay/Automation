#!/bin/bash
# Scheduler: runs cron_task.sh daily at a fixed time
# Default: 08:20 (Asia/Shanghai)

SCHEDULE_HOUR="${SCHEDULE_HOUR:-8}"
SCHEDULE_MIN="${SCHEDULE_MIN:-20}"

echo "[scheduler] Starting, schedule: daily at ${SCHEDULE_HOUR}:$(printf '%02d' $SCHEDULE_MIN)"

while true; do
    # 计算到下次执行时间的秒数
    NOW=$(date '+%H %M')
    NOW_H=$(echo "$NOW" | awk '{print $1}')
    NOW_M=$(echo "$NOW" | awk '{print $2}')
    NOW_TOTAL=$((10#$NOW_H * 60 + 10#$NOW_M))
    TARGET_TOTAL=$((SCHEDULE_HOUR * 60 + SCHEDULE_MIN))

    if [ "$NOW_TOTAL" -lt "$TARGET_TOTAL" ]; then
        WAIT_MIN=$((TARGET_TOTAL - NOW_TOTAL))
    else
        WAIT_MIN=$((1440 - NOW_TOTAL + TARGET_TOTAL))
    fi

    WAIT_SEC=$((WAIT_MIN * 60))
    echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') Sleeping ${WAIT_MIN} minutes until next run..."
    sleep "$WAIT_SEC"

    echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') Running cron_task.sh"
    bash /app/workers/cron_task.sh
    EXIT_CODE=$?
    echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') cron_task.sh exited with code ${EXIT_CODE}"

    # 执行完后等 60 秒避免同一分钟重复触发
    sleep 60
done
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
