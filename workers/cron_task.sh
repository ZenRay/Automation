#!/usr/bin/env bash
# cron_task.sh -- 数据管道定时任务脚本（串行执行）
#
# 任务列表：
#   1. OKR 数据管道 - 支持调度层参数透传（--date / --start / --end）
#   2. CR Trail 商品配置 ETL - 使用 CURRENT_DATE，无需日期参数
#   3. Upgrade After Sale - 包含售后商品、订单商品、门店统计三条链路
#
# 用法：
#   ./cron_task.sh                                     # 默认：today, T-7~T
#   ./cron_task.sh 2026-06-08                          # 指定基准日期
#   ./cron_task.sh 2026-06-08 --start -14 --end 0      # 指定日期+自定义窗口
#   ./cron_task.sh --start -14 --end 0                 # 不指定日期，仅自定义窗口
#
# crontab 示例（每天凌晨 2 点执行）：
#   0 2 * * * /home/ray/Documents/RecentWorks/Automation/workers/cron_task.sh >> /home/ray/Documents/RecentWorks/Automation/logs/cron_task.log 2>&1

set -euo pipefail

# ---------------------------------------------------------------------------
# 时区：确保 date 命令取到正确的日期（cron 环境 locale 最小化）
# ---------------------------------------------------------------------------
export TZ='Asia/Shanghai'

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
# 构建命令参数（在锁文件/venv 之前，dry-run 需要）
# ---------------------------------------------------------------------------
# 调度层参数：--date（基准日期）、--start（窗口起点偏移）、--end（窗口终点偏移）
# 实现层参数（cleanup_buffer / lark_extra_*_days）由代码内部配置，不从脚本透传
ARGS=()
if [ -n "${1:-}" ]; then
    # 第一个参数如果是 YYYY-MM-DD 格式，作为 --date
    if [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
        ARGS+=("--date" "$1")
        shift
    fi
    # 其余参数原样透传（--start, --end）
    ARGS+=("$@")
fi

# ---------------------------------------------------------------------------
# dry-run 模式：DRY_RUN=1 ./okr_cron.sh ... 仅打印命令，不创建锁文件/激活 venv
# ---------------------------------------------------------------------------
if [ "${DRY_RUN:-0}" = "1" ]; then
    echo "[DRY-RUN] Task 1: python -m workers.okr.main ${ARGS[*]:-}"
    echo "[DRY-RUN] Task 2: python -m workers.cr_trail.main"
    echo "[DRY-RUN] Task 3(main): python -m workers.upgrade_after_sale.main --as-start -7 --as-end 0 --order-start -1 --order-end 0 --store-stat-start -7 --store-stat-end 0 --enable-persistence --persistence-dir $PERSISTENCE_DIR/upgrade_after_sale --job-id <date>"
    echo "[DRY-RUN] Task 3(retry-on-fail): python -m workers.upgrade_after_sale.main --as-start -7 --as-end 0 --order-start -1 --order-end 0 --store-stat-start -7 --store-stat-end 0 --enable-persistence --persistence-dir $PERSISTENCE_DIR/upgrade_after_sale --job-id <date> --retry-failed-only"
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
# Task 1: OKR 数据管道
# ---------------------------------------------------------------------------
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 1/3] OKR 数据管道 - START (参数: ${ARGS[*]:-默认})" | tee -a "$CRON_LOG_FILE"

if python -m workers.okr.main "${ARGS[@]}"; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 1/3] OKR 数据管道 - SUCCESS" | tee -a "$CRON_LOG_FILE"
else
    EXIT_CODE=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 1/3] OKR 数据管道 - FAILED (exit_code=$EXIT_CODE)" | tee -a "$CRON_LOG_FILE"
    exit $EXIT_CODE
fi

# ---------------------------------------------------------------------------
# Task 2: CR Trail 商品配置 ETL（使用 CURRENT_DATE，无需日期参数）
# ---------------------------------------------------------------------------
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 2/3] CR Trail ETL - START" | tee -a "$CRON_LOG_FILE"

if python -m workers.cr_trail.main; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 2/3] CR Trail ETL - SUCCESS" | tee -a "$CRON_LOG_FILE"
else
    EXIT_CODE=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 2/3] CR Trail ETL - FAILED (exit_code=$EXIT_CODE)" | tee -a "$CRON_LOG_FILE"
    exit $EXIT_CODE
fi

# ---------------------------------------------------------------------------
# Task 3: Upgrade After Sale（末位执行）
# 规则：先主跑，失败时再补跑 retry_failed_only。
# ---------------------------------------------------------------------------
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 3/3] Upgrade After Sale - START (main run)" | tee -a "$CRON_LOG_FILE"

UA_BASE_ARGS=(
    --date "$RUN_DATE"
    --as-start -7
    --as-end 0
    --order-start -1
    --order-end 0
    --store-stat-start -7
    --store-stat-end 0
    --enable-persistence
    --persistence-dir "$PERSISTENCE_DIR/upgrade_after_sale"
    --job-id "$RUN_DATE"
)

if WORKERS_LOG_LEVEL=INFO python -m workers.upgrade_after_sale.main "${UA_BASE_ARGS[@]}"; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 3/3] Upgrade After Sale - SUCCESS (main run)" | tee -a "$CRON_LOG_FILE"
else
    EXIT_CODE=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 3/3] Upgrade After Sale - FAILED (main run, exit_code=$EXIT_CODE), retry failed rows" | tee -a "$CRON_LOG_FILE"

    if WORKERS_LOG_LEVEL=INFO python -m workers.upgrade_after_sale.main "${UA_BASE_ARGS[@]}" --retry-failed-only; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 3/3] Upgrade After Sale - SUCCESS (retry_failed_only)" | tee -a "$CRON_LOG_FILE"
    else
        RETRY_EXIT_CODE=$?
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] [Task 3/3] Upgrade After Sale - FAILED (retry_failed_only, exit_code=$RETRY_EXIT_CODE)" | tee -a "$CRON_LOG_FILE"
        exit $RETRY_EXIT_CODE
    fi
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========== 全部任务执行完成 ==========" | tee -a "$CRON_LOG_FILE"
