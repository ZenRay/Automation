#!/usr/bin/env bash
# okr_cron.sh -- OKR 数据管道定时更新脚本
#
# 功能：执行 OKR 数据管道，支持调度层参数透传（--date / --start / --end）
# 用法：
#   ./okr_cron.sh                                     # 默认：today, T-7~T
#   ./okr_cron.sh 2026-06-08                          # 指定基准日期
#   ./okr_cron.sh 2026-06-08 --start -14 --end 0      # 指定日期+自定义窗口
#   ./okr_cron.sh --start -14 --end 0                 # 不指定日期，仅自定义窗口
#
# crontab 示例（每天凌晨 2 点执行）：
#   0 2 * * * /home/ray/Documents/RecentWorks/Automation/okr_cron.sh >> /home/ray/Documents/RecentWorks/Automation/logs/okr_cron.log 2>&1

set -euo pipefail

# ---------------------------------------------------------------------------
# 时区：确保 date 命令取到正确的日期（cron 环境 locale 最小化）
# ---------------------------------------------------------------------------
export TZ='Asia/Shanghai'

# ---------------------------------------------------------------------------
# 路径配置
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
VENV_DIR="$PROJECT_DIR/.venv"
LOG_DIR="$PROJECT_DIR/logs"
LOCK_FILE="$PROJECT_DIR/.okr_pipeline.lock"

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
    echo "[DRY-RUN] python -m workers.okr.main ${ARGS[*]:-}"
    exit 0
fi

# ---------------------------------------------------------------------------
# 日志目录（首次运行自动创建）
# ---------------------------------------------------------------------------
mkdir -p "$LOG_DIR"

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
# 执行管道（if 块中 set -e 不触发，确保失败日志能正常打印）
# ---------------------------------------------------------------------------
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 开始执行 OKR 数据管道 (参数: ${ARGS[*]:-默认})"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] 工作目录: $PROJECT_DIR"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Python: $(which python)"

if python -m workers.okr.main "${ARGS[@]}"; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 管道执行成功"
else
    EXIT_CODE=$?
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 管道执行失败 (exit_code=$EXIT_CODE)"
    exit $EXIT_CODE
fi
