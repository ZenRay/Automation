#!/usr/bin/env bash
# cron_task.sh -- 数据管道定时任务脚本（串行执行）
#
# 任务列表：
#   1. OKR 数据管道          (okr)           - 支持调度层参数透传（--date / --start / --end）
#   2. CR Trail 商品配置 ETL  (cr_trail)      - 使用 CURRENT_DATE，无需日期参数
#   3. Upgrade After Sale    (upgrade_after_sale) - 包含售后商品、订单商品、门店统计等九条链路
#
# 用法：
#   ./cron_task.sh                                     # 默认：today, T-7~T, 全部任务
#   ./cron_task.sh 2026-06-08                          # 指定基准日期
#   ./cron_task.sh 2026-06-08 --start -14 --end 0      # 指定日期+自定义窗口
#   ./cron_task.sh --start -14 --end 0                 # 不指定日期，仅自定义窗口
#
# 任务控制参数：
#   --skip-task <name>     跳过指定任务（可多次使用）
#   --only-task <name>     仅运行指定任务（可多次使用，与其他 --only-task 取并集）
#   --help, -h             显示帮助信息
#
#   任务名称：okr | cr_trail | upgrade_after_sale（缩写 ua）
#
# 任务控制示例：
#   ./cron_task.sh --skip-task cr_trail                # 跳过 CR Trail，运行 OKR + Upgrade
#   ./cron_task.sh --skip-task cr_trail --skip-task okr  # 仅运行 Upgrade After Sale
#   ./cron_task.sh --only-task upgrade_after_sale      # 同上，仅运行 Upgrade After Sale
#   ./cron_task.sh --only-task okr --only-task ua      # 运行 OKR + Upgrade（ua=upgrade_after_sale）
#   ./cron_task.sh 2026-07-21 --skip-task cr_trail     # 指定日期 + 跳过 CR Trail
#
# crontab 示例（每天凌晨 2 点执行）：
#   0 2 * * * /home/ray/Documents/RecentWorks/Automation/workers/cron_task.sh >> /home/ray/Documents/RecentWorks/Automation/logs/cron_task.log 2>&1

set -euo pipefail

# ---------------------------------------------------------------------------
# 时区：确保 date 命令取到正确的日期（cron 环境 locale 最小化）
# ---------------------------------------------------------------------------
export TZ='Asia/Shanghai'
set -o pipefail  # 管道退出码取首个失败命令

# ---------------------------------------------------------------------------
# 路径配置
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"  # workers/ -> project root
VENV_DIR="$PROJECT_DIR/.venv"
LOG_DIR="$PROJECT_DIR/logs"
PERSISTENCE_DIR="$PROJECT_DIR/logs/persistence"
LOCK_FILE="$PROJECT_DIR/.pipeline.lock"

# ---------------------------------------------------------------------------
# 帮助信息
# ---------------------------------------------------------------------------
show_help() {
    cat <<'HELP'
用法: cron_task.sh [DATE] [OPTIONS] [-- START/END OPTIONS]

数据管道定时任务脚本（串行执行 3 个任务）

位置参数:
  DATE                      基准日期 (YYYY-MM-DD)，默认 today

任务控制:
  --skip-task <name>        跳过指定任务（可多次使用）
  --only-task <name>        仅运行指定任务（可多次使用）
  -h, --help                显示此帮助信息

  任务名称:
    okr                     Task 1: OKR 数据管道
    cr_trail                Task 2: CR Trail 商品配置 ETL
    upgrade_after_sale      Task 3: Upgrade After Sale（缩写: ua）

调度参数（透传给 OKR 管道）:
  --start <N>               窗口起点偏移（默认 -7）
  --end <N>                 窗口终点偏移（默认 0）

示例:
  cron_task.sh                                  # 全部任务，today
  cron_task.sh --skip-task cr_trail             # 跳过 CR Trail
  cron_task.sh --only-task okr --only-task ua   # 仅 OKR + Upgrade
  cron_task.sh 2026-07-21 --skip-task cr_trail  # 指定日期 + 跳过 CR Trail

环境变量:
  DRY_RUN=1                 仅打印命令，不实际执行
HELP
}

# ---------------------------------------------------------------------------
# 构建命令参数（在锁文件/venv 之前，dry-run 需要）
# ---------------------------------------------------------------------------
# 调度层参数：--date（基准日期）、--start（窗口起点偏移）、--end（窗口终点偏移）
# 实现层参数（cleanup_buffer / lark_extra_*_days）由代码内部配置，不从脚本透传
ARGS=()
SKIP_TASKS=()
ONLY_TASKS=()

# 任务名称规范化：ua -> upgrade_after_sale
_normalize_task_name() {
    case "$1" in
        ua|upgrade_after_sale|upgrade) echo "upgrade_after_sale" ;;
        okr|OKR)                        echo "okr" ;;
        cr_trail|cr|cr_trail)           echo "cr_trail" ;;
        *)                              echo "$1" ;;
    esac
}

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            show_help
            exit 0
            ;;
        --skip-task)
            if [ -z "${2:-}" ]; then
                echo "错误: --skip-task 需要参数 (okr | cr_trail | upgrade_after_sale)" >&2
                exit 1
            fi
            SKIP_TASKS+=("$(_normalize_task_name "$2")")
            shift 2
            ;;
        --only-task)
            if [ -z "${2:-}" ]; then
                echo "错误: --only-task 需要参数 (okr | cr_trail | upgrade_after_sale)" >&2
                exit 1
            fi
            ONLY_TASKS+=("$(_normalize_task_name "$2")")
            shift 2
            ;;
        *)
            # 第一个非 flag 参数如果是 YYYY-MM-DD 格式，作为 --date
            if [[ ${#ARGS[@]} -eq 0 && "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
                ARGS+=("--date" "$1")
            else
                ARGS+=("$1")
            fi
            shift
            ;;
    esac
done

# ---------------------------------------------------------------------------
# 任务开关判定
# ---------------------------------------------------------------------------
_task_enabled() {
    local task_name="$1"
    # 如果指定了 --only-task，则仅运行 only 列表中的任务
    if [ ${#ONLY_TASKS[@]} -gt 0 ]; then
        for t in "${ONLY_TASKS[@]}"; do
            [ "$t" = "$task_name" ] && return 0
        done
        return 1
    fi
    # 否则检查是否在 skip 列表中
    if [ ${#SKIP_TASKS[@]} -gt 0 ]; then
        for t in "${SKIP_TASKS[@]}"; do
            [ "$t" = "$task_name" ] && return 1
        done
    fi
    return 0
}

# ---------------------------------------------------------------------------
# dry-run 模式：DRY_RUN=1 ./cron_task.sh ... 仅打印命令，不创建锁文件/激活 venv
# ---------------------------------------------------------------------------
if [ "${DRY_RUN:-0}" = "1" ]; then
    _task_enabled "okr"                 && echo "[DRY-RUN] Task 1: python -m workers.okr.main ${ARGS[*]:-}" || echo "[DRY-RUN] Task 1: SKIPPED (okr)"
    _task_enabled "cr_trail"            && echo "[DRY-RUN] Task 2: python -m workers.cr_trail.main" || echo "[DRY-RUN] Task 2: SKIPPED (cr_trail)"
    _task_enabled "upgrade_after_sale"  && echo "[DRY-RUN] Task 3: python -m workers.upgrade_after_sale.main <UA_BASE_ARGS>" || echo "[DRY-RUN] Task 3: SKIPPED (upgrade_after_sale)"
    exit 0
fi

# ---------------------------------------------------------------------------
# 日志目录（首次运行自动创建）
# ---------------------------------------------------------------------------
mkdir -p "$LOG_DIR" "$PERSISTENCE_DIR/cron" "$PERSISTENCE_DIR/upgrade_after_sale"

# ---------------------------------------------------------------------------
# 锁文件：防止上一次未完成时重复启动
# ---------------------------------------------------------------------------
if [ -f "$LOCK_FILE" ]; then
    OLD_PID=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] 管道正在运行中 (PID=$OLD_PID)，跳过本次执行"
        exit 0
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] 发现残留锁文件 (PID=$OLD_PID 已不存在)，清除后继续"
        rm -f "$LOCK_FILE"
    fi
fi

echo $$ > "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT

# ---------------------------------------------------------------------------
# 激活虚拟环境
# ---------------------------------------------------------------------------
if [ ! -d "$VENV_DIR" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 错误：虚拟环境不存在 ($VENV_DIR)"
    exit 1
fi

cd "$PROJECT_DIR"
source "$VENV_DIR/bin/activate"

# ---------------------------------------------------------------------------
# 执行管道（串行任务）
# ---------------------------------------------------------------------------
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========== 开始执行数据管道 =========="
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 工作目录: $PROJECT_DIR"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Python: $(which python)"

RUN_DATE="$(date '+%Y-%m-%d')"
for ((i=0; i<${#ARGS[@]}; i++)); do
    if [[ "${ARGS[$i]}" == "--date" && $((i + 1)) -lt ${#ARGS[@]} ]]; then
        RUN_DATE="${ARGS[$((i + 1))]}"
    fi
done

CRON_LOG_FILE="$PERSISTENCE_DIR/cron/cron_task_${RUN_DATE}.log"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cron log file: $CRON_LOG_FILE"

# ---------------------------------------------------------------------------
# 任务统计
# ---------------------------------------------------------------------------
TASKS_RUN=0
TASKS_SKIPPED=0
TASKS_FAILED=0

# ---------------------------------------------------------------------------
# Task 1: OKR 数据管道
# ---------------------------------------------------------------------------
if ! _task_enabled "okr"; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 1/3] OKR 数据管道 - SKIPPED (--skip-task/--only-task)" | tee -a "$CRON_LOG_FILE"
    TASKS_SKIPPED=$((TASKS_SKIPPED + 1))
else

echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 1/3] OKR 数据管道 - START (参数: ${ARGS[*]:-默认})" | tee -a "$CRON_LOG_FILE"

if python -m workers.okr.main "${ARGS[@]}" 2>&1 | tee -a "$CRON_LOG_FILE"; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 1/3] OKR 数据管道 - SUCCESS" | tee -a "$CRON_LOG_FILE"
    TASKS_RUN=$((TASKS_RUN + 1))
else
    EXIT_CODE=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 1/3] OKR 数据管道 - FAILED (exit_code=$EXIT_CODE)" | tee -a "$CRON_LOG_FILE"
    TASKS_RUN=$((TASKS_RUN + 1))
    TASKS_FAILED=$((TASKS_FAILED + 1))
    exit $EXIT_CODE
fi

fi  # _task_enabled okr

# ---------------------------------------------------------------------------
# Task 2: CR Trail 商品配置 ETL（使用 CURRENT_DATE，无需日期参数）
# ---------------------------------------------------------------------------
if ! _task_enabled "cr_trail"; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 2/3] CR Trail ETL - SKIPPED (--skip-task/--only-task)" | tee -a "$CRON_LOG_FILE"
    TASKS_SKIPPED=$((TASKS_SKIPPED + 1))
else

echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 2/3] CR Trail ETL - START" | tee -a "$CRON_LOG_FILE"

if python -m workers.cr_trail.main 2>&1 | tee -a "$CRON_LOG_FILE"; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 2/3] CR Trail ETL - SUCCESS" | tee -a "$CRON_LOG_FILE"
    TASKS_RUN=$((TASKS_RUN + 1))
else
    EXIT_CODE=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 2/3] CR Trail ETL - FAILED (exit_code=$EXIT_CODE)" | tee -a "$CRON_LOG_FILE"
    TASKS_RUN=$((TASKS_RUN + 1))
    TASKS_FAILED=$((TASKS_FAILED + 1))
    exit $EXIT_CODE
fi

fi  # _task_enabled cr_trail

# ---------------------------------------------------------------------------
# Task 3: Upgrade After Sale（末位执行）
# 规则：先主跑，失败时再补跑 retry_failed_only。
# ---------------------------------------------------------------------------
if ! _task_enabled "upgrade_after_sale"; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 3/3] Upgrade After Sale - SKIPPED (--skip-task/--only-task)" | tee -a "$CRON_LOG_FILE"
    TASKS_SKIPPED=$((TASKS_SKIPPED + 1))
else

echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 3/3] Upgrade After Sale - START (main run)" | tee -a "$CRON_LOG_FILE"

UA_BASE_ARGS=(
    --date "$RUN_DATE"
    --as-start -2
    --as-end -1
    --order-start -1
    --order-end 0
    --store-stat-start -2
    --store-stat-end -1
    --store-cat1-stat-start -7
    --store-cat1-stat-end 0
    --cat4-stat-start -10
    --cat4-stat-end 0
    --mct-cat4-stat-start -10
    --mct-cat4-stat-end 0
    --sku-stat-start -15
    --sku-stat-end 0
    --mct-stat-start -15
    --mct-stat-end 0
    --dim-sku-start 0
    --dim-sku-end 0
    --enable-persistence
    --persistence-dir "$PERSISTENCE_DIR/upgrade_after_sale"
    --job-id "$RUN_DATE"
)

if WORKERS_LOG_LEVEL=INFO python -m workers.upgrade_after_sale.main "${UA_BASE_ARGS[@]}" 2>&1 | tee -a "$CRON_LOG_FILE"; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 3/3] Upgrade After Sale - SUCCESS (main run)" | tee -a "$CRON_LOG_FILE"
    TASKS_RUN=$((TASKS_RUN + 1))
else
    EXIT_CODE=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 3/3] Upgrade After Sale - FAILED (main run, exit_code=$EXIT_CODE), retry failed rows" | tee -a "$CRON_LOG_FILE"

    if WORKERS_LOG_LEVEL=INFO python -m workers.upgrade_after_sale.main "${UA_BASE_ARGS[@]}" --retry-failed-only 2>&1 | tee -a "$CRON_LOG_FILE"; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 3/3] Upgrade After Sale - SUCCESS (retry_failed_only)" | tee -a "$CRON_LOG_FILE"
        TASKS_RUN=$((TASKS_RUN + 1))
    else
        RETRY_EXIT_CODE=$?
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 3/3] Upgrade After Sale - FAILED (retry_failed_only, exit_code=$RETRY_EXIT_CODE)" | tee -a "$CRON_LOG_FILE"
        TASKS_RUN=$((TASKS_RUN + 1))
        TASKS_FAILED=$((TASKS_FAILED + 1))
        exit $RETRY_EXIT_CODE
    fi
fi

fi  # _task_enabled upgrade_after_sale

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========== 任务执行完成 (运行: $TASKS_RUN, 跳过: $TASKS_SKIPPED, 失败: $TASKS_FAILED) ==========" | tee -a "$CRON_LOG_FILE"
