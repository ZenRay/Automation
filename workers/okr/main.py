# coding:utf8
"""workers.okr.main -- OKR 主流程编排

串联 ETL 各步骤：
  1. 加载配置（okr.config）
  2. 初始化客户端（LarkMultiDimTable + MaxComputerClient + FieldTypeCoercer）
  3. 拉取所有飞书源 -> lark_data: dict[name, DataFrame]（可选，无配置时跳过）
  4. 执行所有 SQL 查询 -> mc_data: dict[name, DataFrame]
  5. 数据融合 -> merged_df（有 merge 配置时执行，否则直接使用 SQL 结果）
  6. 数据清洗/转换 -> result_df（使用 OKR 注册步骤）
  7. 循环 LARK_TARGETS：清理旧数据 -> 类型转换 -> 写入
  8. 结果校验与日志汇总

每个步骤用 try/except 包裹，失败时记录错误并抛出。
"""

import argparse
import dataclasses
import logging
import sys
from datetime import date as _date, timedelta as _timedelta
from pathlib import Path

import pandas as pd

from automation.conf import lark as lark_conf, maxcomputer as mc_conf
from automation.client import LarkMultiDimTable, MaxComputerClient
from automation import hints as MC_HINTS

from workers.lib import (
    extract_all_lark_sources,
    execute_all_queries,
    FieldTypeCoercer,
    write_to_all_targets,
    DateRangeParams,
    CleanupCondition,
)
from workers.lib.models import LarkSourceConfig, DataRoute
from .config import LARK_SOURCES, SQL_QUERIES, LARK_TARGETS, SQL_BASE_DIR
from .config import DATA_ROUTES
from .transformer import build_okr_transformer, OKR_MERGE_CONFIG, execute_okr_merge

logger = logging.getLogger("workers.okr.main")


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


def _is_merge_config_active(merge_config: dict) -> bool:
    """判断 merge 配置是否有效（非空且不含 TODO 占位符）"""
    if not merge_config:
        return False
    for v in merge_config.values():
        if isinstance(v, str) and "<TODO" in v:
            return False
    return True


def _apply_date_range_to_lark_sources(
    sources: list[LarkSourceConfig],
    date_range: DateRangeParams,
) -> list[LarkSourceConfig]:
    """用 date_range 覆盖各源的日期过滤参数

    根据每个源的独立偏移配置计算实际拉取日期区间：
      - start_date = reference_date + start_offset + src.lark_extra_start_days
      - end_date   = reference_date + end_offset + src.lark_extra_end_days
        （仅当 lark_extra_end_days != 0 时才设置上界，0 表示不限上界）

    所有日期计算在此完成，下游 _apply_date_filter 只负责按显式区间过滤，
    确保 Lark 源过滤窗口与 SQL DATEADD 窗口完全一致。

    只覆盖已配置 date_filter_field 的源，未配置日期筛选的源保持不变。
    """
    ref = (
        date_range.reference_date
        if date_range.reference_date is not None
        else _date.today()
    )
    start_base = ref + _timedelta(days=date_range.start_offset)
    end_base = ref + _timedelta(days=date_range.end_offset)
    result = []
    for src in sources:
        if not src.date_filter_field:
            result.append(src)
            continue
        # 下界：基准起点 + 源的额外偏移（负值=向前回溯）
        start_date = start_base + _timedelta(days=src.lark_extra_start_days)
        # 上界：仅当有偏移时才设置（0 表示不限上界）
        end_date = (
            end_base + _timedelta(days=src.lark_extra_end_days)
            if src.lark_extra_end_days != 0
            else None
        )
        result.append(
            dataclasses.replace(
                src,
                date_filter_start_date=start_date,
                date_filter_end_date=end_date,
            )
        )
    return result


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


def run_okr_pipeline(
    date_range: DateRangeParams = None,
) -> int:
    """执行 OKR 数据处理管道

    Args:
        date_range: 日期范围参数，None 时使用默认值（T-7 到 T）

    Returns:
        int: 0 表示成功，1 表示失败
    """
    if date_range is None:
        date_range = DateRangeParams()  # 默认 T-7 到 T

    from datetime import date as _date

    ref_date = date_range.reference_date or _date.today()
    logger.info("=" * 60)
    logger.info("OKR Data Pipeline - START")
    logger.info(
        f"Date range: T{date_range.start_offset} ~ T{date_range.end_offset} "
        f"(cleanup_days={date_range.cleanup_days}, reference_date={ref_date})"
    )
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # 步骤 1: 初始化客户端
    # ------------------------------------------------------------------
    try:
        logger.info("[Step 1/7] Initializing clients...")
        lark_client = _init_lark_client()
        mc_client = _init_mc_client()
        coercer = FieldTypeCoercer()
        logger.info("All clients initialized successfully")
    except Exception as e:
        logger.error(f"[Step 1/7] Client initialization failed: {e}")
        return 1

    # ------------------------------------------------------------------
    # 步骤 2: 拉取飞书数据源（可选）
    # ------------------------------------------------------------------
    lark_data: dict[str, pd.DataFrame] = {}
    if LARK_SOURCES:
        try:
            # 用 date_range 计算各源的精确日期窗口（start_date/end_date）
            effective_sources = _apply_date_range_to_lark_sources(
                LARK_SOURCES, date_range
            )
            logger.info(
                f"[Step 2/7] Extracting {len(effective_sources)} Lark source(s) "
                f"(date_days={date_range.lark_days})..."
            )
            lark_data = extract_all_lark_sources(lark_client, effective_sources)
            for name, df in lark_data.items():
                logger.info(
                    f"  Lark source '{name}': {df.shape[0]} rows, {df.shape[1]} columns"
                )
        except Exception as e:
            logger.error(f"[Step 2/7] Lark extraction failed: {e}")
            return 1
    else:
        logger.info("[Step 2/7] No Lark sources configured, skipping extraction")

    # ------------------------------------------------------------------
    # 步骤 3: 执行 SQL 查询
    # ------------------------------------------------------------------
    try:
        logger.info(f"[Step 3/7] Executing {len(SQL_QUERIES)} SQL query/queries...")
        mc_data: dict[str, pd.DataFrame] = execute_all_queries(
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
        logger.error(f"[Step 3/7] SQL execution failed: {e}")
        return 1

    # ------------------------------------------------------------------
    # 步骤 4: 数据融合
    # ------------------------------------------------------------------
    try:
        # 优先使用 OKR 专属多步融合逻辑（合并多个飞书源 + MaxCompute 结果）
        # 如需回退到单次 merge，删除以下 if 块，使用 lib.transformer.merge()
        if _is_merge_config_active(OKR_MERGE_CONFIG):
            logger.info(
                "[Step 4/7] Using OKR multi-step merge (Lark sources + MaxCompute)..."
            )
            merged_df = execute_okr_merge(lark_data, mc_data)
        else:
            logger.info("[Step 4/7] No active merge config, using SQL data directly")
            if len(mc_data) == 1:
                merged_df = next(iter(mc_data.values()))
            else:
                first_name = SQL_QUERIES[0].name
                merged_df = mc_data[first_name]
                logger.info(
                    f"  Multiple SQL results, using '{first_name}' as primary ({merged_df.shape})"
                )
        logger.info(
            f"  Data after merge/passthrough: {merged_df.shape[0]} rows, {merged_df.shape[1]} columns"
        )
    except Exception as e:
        logger.error(f"[Step 4/7] Data merge failed: {e}")
        return 1

    # ------------------------------------------------------------------
    # 步骤 5: 数据清洗/转换
    # ------------------------------------------------------------------
    try:
        # execute_okr_merge 内部已执行过一次 transform，跳过重复调用
        if _is_merge_config_active(OKR_MERGE_CONFIG):
            logger.info(
                "[Step 5/7] Transform already done in execute_okr_merge, skipping..."
            )
            result_df = merged_df  # merged_df 已经是 transform 后的结果
        else:
            logger.info("[Step 5/7] Running transform pipeline...")
            transformer = build_okr_transformer()
            result_df = transformer.transform(merged_df)
        logger.info(
            f"  Transform result: {result_df.shape[0]} rows, {result_df.shape[1]} columns"
        )
    except Exception as e:
        logger.error(f"[Step 5/7] Transform pipeline failed: {e}")
        return 1

    # ------------------------------------------------------------------
    # 步骤 6: 写入目标表
    # ------------------------------------------------------------------
    routes_had_failure = False
    try:
        if DATA_ROUTES:
            # 新模式：按路由写入（用 date_range 覆盖清理窗口）
            from workers.lib import DataRouter, SchemaValidator

            effective_routes = _apply_date_range_to_routes(DATA_ROUTES, date_range)
            router = DataRouter(lark_client, coercer, validator=SchemaValidator())
            logger.info(router.describe_routes(effective_routes))
            report = router.route(
                effective_routes,
                lark_data=lark_data,
                mc_data=mc_data,
                result_df=result_df,
            )
            logger.info(f"[Step 6/7] Route completed: {report.summary}")
        else:
            # 旧模式：直接写入 LARK_TARGETS（向后兼容）
            logger.info(f"[Step 6/7] Writing to {len(LARK_TARGETS)} target table(s)...")
            write_to_all_targets(lark_client, result_df, LARK_TARGETS, coercer=coercer)
    except Exception as e:
        logger.error(f"[Step 6/7] Target write had failures: {e}")
        routes_had_failure = True

    # ------------------------------------------------------------------
    # 步骤 7: 结果汇总
    # ------------------------------------------------------------------
    logger.info("[Step 7/7] Pipeline summary:")
    logger.info(f"  Lark sources processed: {len(lark_data)}")
    logger.info(f"  SQL queries executed:   {len(mc_data)}")
    logger.info(f"  Final result rows:      {len(result_df)}")
    logger.info(f"  Final result columns:   {len(result_df.columns)}")
    if DATA_ROUTES:
        logger.info(f"  Routes configured:      {len(DATA_ROUTES)}")
        for route in DATA_ROUTES:
            logger.info(f"    - {route.name} -> {route.target.table_name}")
    else:
        logger.info(f"  Target tables written:  {len(LARK_TARGETS)}")
    logger.info("=" * 60)
    if routes_had_failure:
        logger.warning(
            "OKR Data Pipeline - COMPLETED WITH FAILURES (some routes failed)"
        )
        logger.info("=" * 60)
        return 1
    logger.info("OKR Data Pipeline - COMPLETED SUCCESSFULLY")
    logger.info("=" * 60)

    return 0


def main():
    """入口函数，供命令行调用

    用法：
        python -m workers.okr.main                           # 默认 T-7 ~ T（当天）
        python -m workers.okr.main --date 2026-05-30         # 基准日 2026-05-30
        python -m workers.okr.main --date 2026-05-30 --start -14 --end 0  # 14天窗口
    """
    parser = argparse.ArgumentParser(description="OKR 数据管道")
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
    sys.exit(run_okr_pipeline(date_range=date_range))


if __name__ == "__main__":
    main()
