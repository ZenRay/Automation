#!/usr/bin/env bash
# base_data_cron.sh -- 门店基础数据 SQL-to-SQL ETL 定时任务脚本（单任务）
#
# 任务内容：
#   调用 workers.base_data.main 执行 a3_store.sql，
#   INSERT OVERWRITE 写入 MaxCompute 目标表分区（纯 SQL 执行，不写飞书、不做数据同步）。
#
# 与 cron_task.sh 的关系：
#   - cron_task.sh     : OKR / Daily Report / Upgrade After Sale 三链路（默认 08:20 触发）
#   - base_data_cron.sh: 门店基础数据 ETL（默认 07:20 触发，与 cron_task.sh 错开）
#   两者使用各自独立的锁文件，互不阻塞。
#
# 用法：
#   ./base_data_cron.sh                          # 基准日=今天，写入 dt=今天 分区
#   ./base_data_cron.sh 2026-09-21               # 指定基准日期（位置参数）
#   ./base_data_cron.sh --date 2026-09-21        # 指定基准日期（flag 形式）
#   ./base_data_cron.sh --date 2026-09-21 --end -1   # 写入 dt=基准日-1 分区
#
# 参数：
#   --date <YYYY-MM-DD>   基准日期，默认今天；也可直接作为第一个位置参数
#   --end <N>             结束偏移量 end_offset（默认 0），目标分区 dt = 基准日 + end_offset
#   --start <N>           【已忽略】base_data.main 不接受 --start（start_offset 恒为 0，
#                         SQL 仅用 date_param 与 end_offset 渲染），透传会导致 argparse 报错
#   -h, --help            显示帮助信息
#
# 环境变量：
#   DRY_RUN=1             仅打印命令，不实际执行（不创建锁文件 / 不激活 venv）
#
# crontab / scheduler 示例（每天 07:20 执行）：
#   见 deploy/scripts/scheduler.sh 的 base_data 任务配置

set -euo pipefail

# ---------------------------------------------------------------------------
# 时区：确保 date 命令取到正确的日期（cron/scheduler 环境 locale 最小化）
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
# 独立锁文件：与 cron_task.sh 的 .pipeline.lock 分开，两个任务互不阻塞
LOCK_FILE="$PROJECT_DIR/.base_data.lock"

# ---------------------------------------------------------------------------
# 帮助信息
# ---------------------------------------------------------------------------
show_help() {
    cat <<'HELP'
用法: base_data_cron.sh [DATE] [OPTIONS]

门店基础数据 SQL-to-SQL ETL 定时任务（调用 workers.base_data.main）

位置参数:
  DATE                      基准日期 (YYYY-MM-DD)，默认 today

选项:
  --date <YYYY-MM-DD>       基准日期，默认 today
  --end <N>                 结束偏移量 end_offset（默认 0）
                            目标分区 dt = 基准日期 + end_offset
  --start <N>               【已忽略】base_data.main 不支持 --start
  -h, --help                显示此帮助信息

示例:
  base_data_cron.sh                             # 基准日=今天，写入 dt=今天
  base_data_cron.sh 2026-09-21                  # 指定基准日期
  base_data_cron.sh --date 2026-09-21 --end -1  # 写入 dt=基准日-1 分区

环境变量:
  DRY_RUN=1                 仅打印命令，不实际执行
HELP
}

# ---------------------------------------------------------------------------
# 参数解析
# ---------------------------------------------------------------------------
RUN_DATE=""
END_OFFSET=""

while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help)
            show_help
            exit 0
            ;;
        --date)
            if [ -z "${2:-}" ]; then
                echo "错误: --date 需要参数 (YYYY-MM-DD)" >&2
                exit 1
            fi
            RUN_DATE="$2"
            shift 2
            ;;
        --end)
            if [ -z "${2:-}" ]; then
                echo "错误: --end 需要参数 (整数)" >&2
                exit 1
            fi
            END_OFFSET="$2"
            shift 2
            ;;
        --start)
            # base_data.main 不接受 --start：告警并忽略，避免 argparse 报错
            echo "[warn] base_data.main 不支持 --start（start_offset 恒为 0），已忽略: ${2:-}" >&2
            if [ -z "${2:-}" ]; then
                shift
            else
                shift 2
            fi
            ;;
        *)
            # 第一个非 flag 参数如果是 YYYY-MM-DD 格式，作为 --date
            if [[ -z "$RUN_DATE" && "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
                RUN_DATE="$1"
            else
                echo "错误: 未知参数 '$1'" >&2
                show_help
                exit 1
            fi
            shift
            ;;
    esac
done

# ---------------------------------------------------------------------------
# 构建 python 命令参数（仅透传 base_data.main 支持的 --date / --end）
# ---------------------------------------------------------------------------
PY_ARGS=()
[ -n "$RUN_DATE" ]    && PY_ARGS+=("--date" "$RUN_DATE")
[ -n "$END_OFFSET" ]  && PY_ARGS+=("--end" "$END_OFFSET")

# ---------------------------------------------------------------------------
# dry-run 模式：DRY_RUN=1 ./base_data_cron.sh ... 仅打印命令，不创建锁/激活 venv
# ---------------------------------------------------------------------------
if [ "${DRY_RUN:-0}" = "1" ]; then
    echo "[DRY-RUN] python -m workers.base_data.main ${PY_ARGS[*]:-}"
    exit 0
fi

# ---------------------------------------------------------------------------
# 日志目录（首次运行自动创建）
# ---------------------------------------------------------------------------
mkdir -p "$LOG_DIR" "$PERSISTENCE_DIR/cron"

# ---------------------------------------------------------------------------
# 锁文件：防止上一次未完成时重复启动
# ---------------------------------------------------------------------------
if [ -f "$LOCK_FILE" ]; then
    OLD_PID=$(cat "$LOCK_FILE" 2>/dev/null || echo "")
    if [ -n "$OLD_PID" ] && kill -0 "$OLD_PID" 2>/dev/null; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] base_data 任务正在运行中 (PID=$OLD_PID)，跳过本次执行"
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
# 执行 ETL
# ---------------------------------------------------------------------------
LOG_DATE="${RUN_DATE:-$(date '+%Y-%m-%d')}"
CRON_LOG_FILE="$PERSISTENCE_DIR/cron/base_data_${LOG_DATE}.log"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========== 开始执行 base_data ETL ==========" | tee -a "$CRON_LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 工作目录: $PROJECT_DIR" | tee -a "$CRON_LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Python: $(which python)" | tee -a "$CRON_LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Cron log file: $CRON_LOG_FILE" | tee -a "$CRON_LOG_FILE"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 参数: ${PY_ARGS[*]:-默认}" | tee -a "$CRON_LOG_FILE"

if python -m workers.base_data.main ${PY_ARGS[@]+"${PY_ARGS[@]}"} 2>&1 | tee -a "$CRON_LOG_FILE"; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] base_data ETL - SUCCESS" | tee -a "$CRON_LOG_FILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========== 任务执行完成 ==========" | tee -a "$CRON_LOG_FILE"
    exit 0
else
    EXIT_CODE=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] base_data ETL - FAILED (exit_code=$EXIT_CODE)" | tee -a "$CRON_LOG_FILE"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ========== 任务执行失败 ==========" | tee -a "$CRON_LOG_FILE"
    exit 1
fi
