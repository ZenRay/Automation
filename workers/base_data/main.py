# coding:utf8
"""workers.base_data.main -- 门店基础数据 ETL 主流程编排

独立的 SQL-to-SQL ETL 任务，串联步骤：
  1. 初始化 MaxCompute 客户端
  2. 渲染日期参数（${date_param} / ${end_offset}）
  3. 执行 a3_store.sql：INSERT OVERWRITE 写入目标表 dt 分区
  4. 结果日志汇总

说明：
    SQL 内部已在 MaxCompute 侧完成 提取 → 转换 → 写入 全流程，
    无结果集返回、不写飞书，因此本流程不包含 Lark 客户端与路由，
    不添加任何额外业务逻辑处理。

每个步骤用 try/except 包裹，失败时记录错误并返回非零退出码。
"""

import argparse
import logging
import sys
import time
from datetime import date as _date, timedelta as _timedelta

from automation.conf import maxcomputer as mc_conf
from automation.client import MaxComputerClient
from automation import hints as MC_HINTS

from workers.lib import execute_all_queries, DateRangeParams
from .config import SQL_BASE_DIR, SQL_QUERIES, TARGET_TABLE

logger = logging.getLogger("workers.base_data.main")


def _init_mc_client() -> MaxComputerClient:
    """初始化 MaxCompute 客户端（prod 环境）"""
    conf = {
        "access_id": mc_conf.get("prod", "access_id"),
        "secret_access_key": mc_conf.get("prod", "secret_access_key"),
        "project": mc_conf.get("prod", "project"),
        "endpoint": mc_conf.get("prod", "endpoint"),
    }
    logger.info(f"Initializing MaxComputerClient (project={conf['project']})")
    return MaxComputerClient(**conf)


def run_base_data_pipeline(
    date_range: DateRangeParams = None,
) -> int:
    """执行门店基础数据 ETL 管道

    Args:
        date_range: 日期范围参数，None 时使用默认值（end_offset=0）

    Returns:
        int: 0 表示成功，1 表示失败
    """
    if date_range is None:
        date_range = DateRangeParams(start_offset=0, end_offset=0)

    ref_date = date_range.reference_date or _date.today()
    target_dt = ref_date + _timedelta(days=date_range.end_offset)
    logger.info("=" * 60)
    logger.info("Base Data Pipeline (a3_store) - START")
    logger.info(
        f"Reference date: {ref_date}, end_offset: T{date_range.end_offset}, "
        f"target partition dt: {target_dt}"
    )
    logger.info(f"Target table: {TARGET_TABLE}")
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # 步骤 1: 初始化客户端
    # ------------------------------------------------------------------
    try:
        logger.info("[Step 1/3] Initializing MaxCompute client...")
        mc_client = _init_mc_client()
        logger.info("MaxCompute client initialized successfully")
    except Exception as e:
        logger.error(f"[Step 1/3] Client initialization failed: {e}")
        return 1

    # ------------------------------------------------------------------
    # 步骤 2: 执行 SQL（execute_only：INSERT OVERWRITE 写入目标表分区）
    # ------------------------------------------------------------------
    started_at = time.time()
    try:
        logger.info(f"[Step 2/3] Executing {len(SQL_QUERIES)} SQL statement(s)...")
        mc_data = execute_all_queries(
            mc_client,
            SQL_QUERIES,
            SQL_BASE_DIR,
            hints=MC_HINTS,
            params=date_range.sql_params(),
        )
        elapsed = time.time() - started_at
        for name in mc_data:
            logger.info(f"  SQL statement '{name}' completed in {elapsed:.1f}s")
    except Exception as e:
        logger.error(f"[Step 2/3] SQL execution failed: {e}")
        return 1

    # ------------------------------------------------------------------
    # 步骤 3: 结果汇总
    # ------------------------------------------------------------------
    logger.info("[Step 3/3] Pipeline summary:")
    logger.info(f"  SQL statements executed: {len(SQL_QUERIES)}")
    for query in SQL_QUERIES:
        logger.info(f"    - {query.name} -> {TARGET_TABLE} (dt={target_dt})")
    logger.info("=" * 60)
    logger.info("Base Data Pipeline (a3_store) - COMPLETED SUCCESSFULLY")
    logger.info("=" * 60)

    return 0


def main():
    """入口函数，供命令行调用

    用法：
        python -m workers.base_data.main                        # 基准日=今天，写入 dt=今天 分区
        python -m workers.base_data.main --date 2026-09-21      # 基准日 2026-09-21，写入 dt=2026-09-21 分区
        python -m workers.base_data.main --end -1               # 写入 dt=基准日-1 分区
    """
    parser = argparse.ArgumentParser(description="门店基础数据 ETL 管道（a3_store）")
    parser.add_argument(
        "--date", type=str, default=None, help="基准日期 (YYYY-MM-DD)，默认今天"
    )
    parser.add_argument(
        "--end",
        type=int,
        default=0,
        help="结束偏移量 end_offset (默认 0)，目标分区 dt = 基准日期 + end_offset",
    )
    args = parser.parse_args()

    ref_date = None
    if args.date:
        ref_date = _date.fromisoformat(args.date)

    date_range = DateRangeParams(
        start_offset=0,  # SQL 仅使用 date_param 与 end_offset，start_offset 不参与渲染
        end_offset=args.end,
        date_param=f"DATE '{ref_date}'" if ref_date else "CURRENT_DATE()",
        reference_date=ref_date,
    )
    sys.exit(run_base_data_pipeline(date_range=date_range))


if __name__ == "__main__":
    main()
