# coding:utf8
"""workers.daily_report.main -- 日报（Daily Report）主流程编排

串联 ETL 各步骤：
  1. 加载配置（daily_report.config）
  2. 初始化客户端（LarkMultiDimTable + MaxComputerClient + FieldTypeCoercer）
  3. 执行所有 SQL 查询 -> mc_data: dict[name, DataFrame]
  4. 按路由写入目标飞书多维表格
  5. 结果校验与日志汇总

每个步骤用 try/except 包裹，失败时记录错误并抛出。
"""

import argparse
import dataclasses
import logging
import sys
from datetime import date as _date, timedelta as _timedelta

from automation.conf import lark as lark_conf, maxcomputer as mc_conf
from automation.client import LarkMultiDimTable, MaxComputerClient
from automation import hints as MC_HINTS

from workers.lib import (
    execute_all_queries,
    FieldTypeCoercer,
    DateRangeParams,
    CleanupCondition,
)
from workers.lib.models import DataRoute
from .config import (
    SQL_QUERIES,
    LARK_TARGETS,
    SQL_BASE_DIR,
    DATA_ROUTES,
)

logger = logging.getLogger("workers.daily_report.main")


def _init_lark_client() -> LarkMultiDimTable:
    """初始化飞书多维表格客户端（prod 环境）"""
    app_id = lark_conf.get("prod", "APP_ID")
    app_secret = lark_conf.get("prod", "APP_SECRET")
    lark_host = lark_conf.get("prod", "LARK_HOST", fallback="https://open.feishu.cn")

    logger.info(
        f"Initializing LarkMultiDimTable client (app_id={app_id}, host={lark_host})"
    )
    return LarkMultiDimTable(
        app_id=app_id,
        app_secret=app_secret,
        lark_host=lark_host,
    )


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


def _apply_date_range_to_routes(
    routes: list[DataRoute],
    date_range: DateRangeParams,
) -> list[DataRoute]:
    """用 date_range.cleanup_window 替换路由目标中的运行时窗口哨兵

    三种 cleanup_conditions 处理逻辑：
      - None                              : 跳过（目标不需要清理）
      - CleanupCondition.runtime_window(): 替换为运行时计算的 date_window
      - 其他具体条件                       : 保持原样，不覆盖（尊重配置中的显式定义）

    清理窗口 = [reference_date + start_offset - buffer,
                reference_date + end_offset]
    双边条件确保只删除管道处理窗口内的数据，窗口外不受影响。
    """
    cleanup_start, cleanup_end = date_range.cleanup_window
    logger.info(
        f"Cleanup window: {cleanup_start} ~ {cleanup_end} "
        f"(buffer={date_range.cleanup_buffer})"
    )
    result = []
    for route in routes:
        target = route.target
        if (
            target.cleanup_conditions is not None
            and target.cleanup_conditions.is_runtime
        ):
            # 运行时哨兵：替换为精确窗口
            new_cleanup = CleanupCondition.date_window(
                "日期",
                cleanup_start,
                cleanup_end,
            )
            new_target = dataclasses.replace(target, cleanup_conditions=new_cleanup)
            result.append(dataclasses.replace(route, target=new_target))
        else:
            # None（不清理）或显式条件（尊重配置）：保持不变
            result.append(route)
    return result


def run_daily_report_pipeline(
    date_range: DateRangeParams = None,
) -> int:
    """执行日报数据处理管道

    Args:
        date_range: 日期范围参数，None 时使用默认值（T-7 到 T）

    Returns:
        int: 0 表示成功，1 表示失败
    """
    if date_range is None:
        date_range = DateRangeParams()  # 默认 T-7 到 T

    ref_date = date_range.reference_date or _date.today()
    logger.info("=" * 60)
    logger.info("Daily Report Data Pipeline - START")
    logger.info(
        f"Date range: T{date_range.start_offset} ~ T{date_range.end_offset} "
        f"(cleanup_days={date_range.cleanup_days}, reference_date={ref_date})"
    )
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # 步骤 1: 初始化客户端
    # ------------------------------------------------------------------
    try:
        logger.info("[Step 1/4] Initializing clients...")
        lark_client = _init_lark_client()
        mc_client = _init_mc_client()
        coercer = FieldTypeCoercer()
        logger.info("All clients initialized successfully")
    except Exception as e:
        logger.error(f"[Step 1/4] Client initialization failed: {e}")
        return 1

    # ------------------------------------------------------------------
    # 步骤 2: 执行 SQL 查询
    # ------------------------------------------------------------------
    try:
        logger.info(f"[Step 2/4] Executing {len(SQL_QUERIES)} SQL query/queries...")
        mc_data = execute_all_queries(
            mc_client,
            SQL_QUERIES,
            SQL_BASE_DIR,
            hints=MC_HINTS,
            params=date_range.sql_params(),
        )
        for name, df in mc_data.items():
            logger.info(
                f"  SQL query '{name}': {df.shape[0]} rows, {df.shape[1]} columns"
            )
    except Exception as e:
        logger.error(f"[Step 2/4] SQL execution failed: {e}")
        return 1

    # ------------------------------------------------------------------
    # 步骤 3: 写入目标表（按路由模式）
    # ------------------------------------------------------------------
    routes_had_failure = False
    try:
        if DATA_ROUTES:
            # 路由模式：按路由配置写入各目标表
            from workers.lib import DataRouter, SchemaValidator

            effective_routes = _apply_date_range_to_routes(DATA_ROUTES, date_range)
            router = DataRouter(lark_client, coercer, validator=SchemaValidator())
            logger.info(router.describe_routes(effective_routes))
            report = router.route(
                effective_routes,
                mc_data=mc_data,
                lark_data={},
                file_data={},
                result_df=None,
            )
            logger.info(f"[Step 3/4] Route completed: {report.summary}")
        else:
            # 回退模式：直接写入 LARK_TARGETS
            from workers.lib import write_to_all_targets

            logger.info(f"[Step 3/4] Writing to {len(LARK_TARGETS)} target table(s)...")
            # 取第一个 SQL 结果作为主数据（回退模式仅适用于单 SQL 场景）
            if mc_data:
                first_name = SQL_QUERIES[0].name
                result_df = mc_data[first_name]
                write_to_all_targets(
                    lark_client, result_df, LARK_TARGETS, coercer=coercer
                )
            else:
                logger.warning("No SQL data available, skipping write")
    except Exception as e:
        logger.error(f"[Step 3/4] Target write had failures: {e}")
        routes_had_failure = True

    # ------------------------------------------------------------------
    # 步骤 4: 结果汇总
    # ------------------------------------------------------------------
    logger.info("[Step 4/4] Pipeline summary:")
    logger.info(f"  SQL queries executed:   {len(mc_data)}")
    if DATA_ROUTES:
        logger.info(f"  Routes configured:      {len(DATA_ROUTES)}")
        for route in DATA_ROUTES:
            logger.info(f"    - {route.name} -> {route.target.table_name}")
    else:
        logger.info(f"  Target tables written:  {len(LARK_TARGETS)}")
    logger.info("=" * 60)
    if routes_had_failure:
        logger.warning(
            "Daily Report Data Pipeline - COMPLETED WITH FAILURES (some routes failed)"
        )
        logger.info("=" * 60)
        return 1
    logger.info("Daily Report Data Pipeline - COMPLETED SUCCESSFULLY")
    logger.info("=" * 60)

    return 0


def main():
    """入口函数，供命令行调用

    用法：
        python -m workers.daily_report.main                           # 默认 T-7 ~ T（当天）
        python -m workers.daily_report.main --date 2026-05-30         # 基准日 2026-05-30
        python -m workers.daily_report.main --date 2026-05-30 --start -14 --end 0  # 14天窗口
    """
    parser = argparse.ArgumentParser(description="日报（Daily Report）数据管道")
    parser.add_argument(
        "--date", type=str, default=None, help="基准日期 (YYYY-MM-DD)，默认今天"
    )
    parser.add_argument("--start", type=int, default=-7, help="起始日偏移量 (默认 -7)")
    parser.add_argument("--end", type=int, default=0, help="结束日偏移量 (默认 0)")
    parser.add_argument(
        "--buffer", type=int, default=0, help="清理窗口额外回溯天数 (默认 0)"
    )
    args = parser.parse_args()

    ref_date = None
    if args.date:
        ref_date = _date.fromisoformat(args.date)

    date_range = DateRangeParams(
        start_offset=args.start,
        end_offset=args.end,
        cleanup_buffer=args.buffer,
        date_param=f"DATE '{ref_date}'" if ref_date else "CURRENT_DATE()",
        reference_date=ref_date,
    )
    sys.exit(run_daily_report_pipeline(date_range=date_range))


if __name__ == "__main__":
    main()
