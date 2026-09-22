#!/usr/bin/env bash
# Scheduler: run daily jobs at fixed times (Asia/Shanghai)
#
# 单进程多任务调度：一个 while 循环内维护多个「每日触发时刻 -> 脚本」任务，
# 每 30s 轮询一次，到点触发对应脚本；每个任务当天只触发一次（按日期去重）。
#
# 当前任务：
#   - base_data_cron.sh : BASE_DATA_SCHEDULE_HOUR:BASE_DATA_SCHEDULE_MIN (默认 07:20)
#                         门店基础数据 SQL-to-SQL ETL（纯 SQL，不写飞书）
#   - cron_task.sh      : SCHEDULE_HOUR:SCHEDULE_MIN                     (默认 07:30，生产 08:20)
#                         OKR / Daily Report / Upgrade After Sale 三链路
#
# 启动语义：若容器在某任务当天触发时刻「之后」启动，则该任务当天不再补跑，
#           等到次日再触发（与旧版单任务 scheduler 行为一致，避免部署即重跑）。

set -euo pipefail

export TZ='Asia/Shanghai'

# ---------------------------------------------------------------------------
# Job 1: base_data_cron.sh（默认 07:20）
# ---------------------------------------------------------------------------
BASE_DATA_SCHEDULE_HOUR="${BASE_DATA_SCHEDULE_HOUR:-7}"
BASE_DATA_SCHEDULE_MIN="${BASE_DATA_SCHEDULE_MIN:-20}"

# ---------------------------------------------------------------------------
# Job 2: cron_task.sh（默认 07:30，生产环境注入 08:20）
# ---------------------------------------------------------------------------
SCHEDULE_HOUR="${SCHEDULE_HOUR:-7}"
SCHEDULE_MIN="${SCHEDULE_MIN:-30}"

# 逗号分隔的任务名，传递给 cron_task.sh --skip-task
# 默认跳过 cr_trail（数据量超飞书 5 万行限制，待修复后移除此默认值）
CRON_SKIP_TASKS="${CRON_SKIP_TASKS:-cr_trail}"

# workers 脚本目录与轮询间隔（秒）
WORKERS_DIR="${WORKERS_DIR:-/app/workers}"
POLL_INTERVAL="${POLL_INTERVAL:-30}"

# 构建 cron_task.sh 的 --skip-task 参数数组
SKIP_ARGS=()
if [ -n "$CRON_SKIP_TASKS" ]; then
    IFS=',' read -ra _tasks <<< "$CRON_SKIP_TASKS"
    for _t in "${_tasks[@]}"; do
        SKIP_ARGS+=("--skip-task" "$_t")
    done
    echo "[scheduler] Skip tasks: $CRON_SKIP_TASKS"
fi

# ---------------------------------------------------------------------------
# 任务注册表
# ---------------------------------------------------------------------------
JOB_IDS=("base_data" "cron_task")
declare -A JOB_MIN JOB_DESC JOB_LASTRUN

JOB_MIN[base_data]=$((10#$BASE_DATA_SCHEDULE_HOUR * 60 + 10#$BASE_DATA_SCHEDULE_MIN))
JOB_DESC[base_data]="base_data_cron.sh @ ${BASE_DATA_SCHEDULE_HOUR}:$(printf '%02d' "$BASE_DATA_SCHEDULE_MIN")"

JOB_MIN[cron_task]=$((10#$SCHEDULE_HOUR * 60 + 10#$SCHEDULE_MIN))
JOB_DESC[cron_task]="cron_task.sh @ ${SCHEDULE_HOUR}:$(printf '%02d' "$SCHEDULE_MIN")"

echo "[scheduler] Starting with ${#JOB_IDS[@]} daily job(s):"
for id in "${JOB_IDS[@]}"; do
    echo "[scheduler]   - ${JOB_DESC[$id]}"
done

# ---------------------------------------------------------------------------
# 单任务执行
# ---------------------------------------------------------------------------
_run_job() {
    local id="$1"
    local ec=0
    case "$id" in
        base_data)
            echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') Running base_data_cron.sh"
            bash "$WORKERS_DIR/base_data_cron.sh" || ec=$?
            ;;
        cron_task)
            echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') Running cron_task.sh"
            bash "$WORKERS_DIR/cron_task.sh" ${SKIP_ARGS[@]+"${SKIP_ARGS[@]}"} || ec=$?
            ;;
        *)
            echo "[scheduler] Unknown job id: $id"
            return 0
            ;;
    esac
    if [ "$ec" -eq 0 ]; then
        echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') ${JOB_DESC[$id]} finished successfully"
    else
        echo "[scheduler] $(date '+%Y-%m-%d %H:%M:%S') ${JOB_DESC[$id]} failed (exit_code=${ec})"
    fi
    return 0
}

# ---------------------------------------------------------------------------
# 启动时：对「当天触发时刻已过」的任务标记为已运行，避免部署即补跑
# ---------------------------------------------------------------------------
_startup_total=$((10#$(date '+%H') * 60 + 10#$(date '+%M')))
_startup_date=$(date '+%Y-%m-%d')
for id in "${JOB_IDS[@]}"; do
    if [[ "$_startup_total" -ge "${JOB_MIN[$id]}" ]]; then
        JOB_LASTRUN["$id"]="$_startup_date"
        echo "[scheduler] '${JOB_DESC[$id]}' already past for today at startup; next run tomorrow"
    fi
done

# ---------------------------------------------------------------------------
# 主循环：轮询触发
# ---------------------------------------------------------------------------
while true; do
    now_total=$((10#$(date '+%H') * 60 + 10#$(date '+%M')))
    today=$(date '+%Y-%m-%d')

    for id in "${JOB_IDS[@]}"; do
        if [[ "${JOB_LASTRUN[$id]:-}" != "$today" && "$now_total" -ge "${JOB_MIN[$id]}" ]]; then
            JOB_LASTRUN["$id"]="$today"
            _run_job "$id"
        fi
    done

    sleep "$POLL_INTERVAL"
done
