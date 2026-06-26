# coding:utf8
"""workers.cr_trail.main -- CR试验配置 主流程编排

串联 ETL 各步骤：
  1. 加载配置（cr_trail.config）
  2. 初始化客户端（LarkMultiDimTable + MaxComputerClient + FieldTypeCoercer）
    3. 拉取本地文件源（可选）→ local_data: dict[name, DataFrame]
    4. 执行所有 SQL 查询 → mc_data: dict[name, DataFrame]，逐个 transform
    5. 路由写入：per-route 日期作用域清理 → 类型转换 → 写入飞书多维表格
    6. 结果校验与日志汇总

多 SQL 多路由架构：
  - 每个 SQL 查询独立执行，结果存入 mc_data[query_name]
  - 每条 DataRoute 通过 source_ref="mc:<query_name>" 引用对应的 DataFrame
  - 日期作用域清理 per-route：每条路由根据自身源数据的日期范围独立计算清理窗口
  - 设计原理：不同 SQL 可能产出不同日期范围，共享清理窗口会导致误删

与 OKR 模块的设计差异：
  - 无 LARK_SOURCES 提取步骤（数据仅来自 MaxCompute）
  - 无多源融合逻辑（SQL 结果直接路由到目标表）
  - 无日期窗口参数（SQL 使用 MAX_PT 获取最新分区快照）

每个步骤用 try/except 包裹，失败时记录错误并抛出。
"""

import argparse
import dataclasses
import logging
import sys
from datetime import date as _date

import pandas as pd

from automation.conf import lark as lark_conf, maxcomputer as mc_conf
from automation.client import LarkMultiDimTable, MaxComputerClient
from automation import hints as MC_HINTS

from workers.lib import (
    extract_all_local_sources,
    execute_all_queries,
    FieldTypeCoercer,
    DataRouter,
    SchemaValidator,
    DateRangeParams,
    CleanupCondition,
)
from workers.lib.local_attachment_preprocessor import preprocess_local_attachment_columns
from workers.lib.models import DataRoute
from .config import SQL_QUERIES, LARK_TARGETS, SQL_BASE_DIR, DATA_ROUTES, LOCAL_FILE_SOURCES
from .transformer import build_cr_trail_transformer

logger = logging.getLogger("workers.cr_trail.main")


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


def _apply_date_scoped_cleanup(
    routes: list[DataRoute],
    result_df: pd.DataFrame,
    date_col: str = "日期",
) -> list[DataRoute]:
    """根据 SQL 结果的实际日期动态生成清理窗口

    核心设计：
    - 从 result_df 中提取日期范围 [min_date, max_date]
    - 将 routes 中的 runtime_window() 哨兵替换为该精确日期窗口
    - 写入前仅删除该日期范围内的旧记录，写入新数据后实现幂等更新
    - 不影响其他历史日期的数据（日志表不能全量清空）

    与 OKR 的 _apply_date_range_to_routes 的区别：
    - OKR 的清理窗口来自 DateRangeParams（管道调度参数）
    - CR Trail 的清理窗口来自 SQL 结果的实际日期（数据驱动）

    Args:
        routes:    DataRoute 配置列表
        result_df: SQL 查询结果 DataFrame
        date_col:  日期列名，默认为 "日期"

    Returns:
        list[DataRoute]: cleanup_conditions 已替换的路由列表
    """
    if result_df.empty or date_col not in result_df.columns:
        logger.warning(
            f"Cannot determine date scope for cleanup: "
            f"df.empty={result_df.empty}, '{date_col}' in cols={date_col in result_df.columns}. "
            "Falling back to no cleanup."
        )
        return [
            (
                dataclasses.replace(
                    r, target=dataclasses.replace(r.target, cleanup_conditions=None)
                )
                if r.target.cleanup_conditions
                and r.target.cleanup_conditions.is_runtime
                else r
            )
            for r in routes
        ]

    # 提取日期范围：兼容 datetime64、date、str 多种格式
    dates = pd.to_datetime(result_df[date_col], errors="coerce").dropna()
    if dates.empty:
        logger.warning(
            f"No valid dates found in '{date_col}', falling back to no cleanup"
        )
        return [
            (
                dataclasses.replace(
                    r, target=dataclasses.replace(r.target, cleanup_conditions=None)
                )
                if r.target.cleanup_conditions
                and r.target.cleanup_conditions.is_runtime
                else r
            )
            for r in routes
        ]

    min_date = dates.min().date()
    max_date = dates.max().date()
    logger.info(
        f"Date-scoped cleanup window: [{min_date}, {max_date}] "
        f"({(max_date - min_date).days + 1} day(s))"
    )

    result = []
    for route in routes:
        if (
            route.target.cleanup_conditions
            and route.target.cleanup_conditions.is_runtime
        ):
            # 替换 runtime_window 哨兵为精确日期窗口
            new_cleanup = CleanupCondition.date_window(date_col, min_date, max_date)
            new_target = dataclasses.replace(
                route.target, cleanup_conditions=new_cleanup
            )
            result.append(dataclasses.replace(route, target=new_target))
        else:
            result.append(route)
    return result


def run_cr_trail_pipeline() -> int:
    """执行 CR试验商品配置 数据处理管道

    数据流：
      MaxCompute SQL (conf_goods.sql, MAX_PT 最新分区)
        → 全量覆盖写入飞书 "conf_商品信息" 表

    Returns:
        int: 0 表示成功，1 表示失败
    """
    logger.info("=" * 60)
    logger.info("CR Trail - 商品配置 ETL Pipeline - START")
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # 步骤 1: 初始化客户端
    # ------------------------------------------------------------------
    try:
        logger.info("[Step 1/5] Initializing clients...")
        lark_client = _init_lark_client()
        mc_client = _init_mc_client()
        coercer = FieldTypeCoercer()
        logger.info("All clients initialized successfully")
    except Exception as e:
        logger.error(f"[Step 1/5] Client initialization failed: {e}")
        return 1

    # ------------------------------------------------------------------
    # 步骤 2: 拉取本地文件数据源（可选）
    # ------------------------------------------------------------------
    local_data: dict[str, pd.DataFrame] = {}
    if LOCAL_FILE_SOURCES:
        try:
            logger.info(
                f"[Step 2/6] Extracting {len(LOCAL_FILE_SOURCES)} local source(s)..."
            )
            local_data = extract_all_local_sources(LOCAL_FILE_SOURCES)
            source_config_map = {cfg.name: cfg for cfg in LOCAL_FILE_SOURCES}
            for name, df in list(local_data.items()):
                cfg = source_config_map.get(name)
                if cfg is not None and cfg.attachment_columns:
                    local_data[name] = preprocess_local_attachment_columns(df, cfg)
                logger.info(
                    f"  Local source '{name}': {local_data[name].shape[0]} rows, "
                    f"{local_data[name].shape[1]} columns"
                )
        except Exception as e:
            logger.error(f"[Step 2/6] Local file extraction failed: {e}")
            return 1
    else:
        logger.info("[Step 2/6] No local file sources configured, skipping extraction")

    # ------------------------------------------------------------------
    # 步骤 3: 执行 SQL 查询
    # ------------------------------------------------------------------
    try:
        logger.info(f"[Step 3/6] Executing {len(SQL_QUERIES)} SQL query/queries...")
        # 使用空 DateRangeParams 生成默认 sql_params，SQL 中 MAX_PT 不依赖日期参数
        default_params = DateRangeParams().sql_params()
        mc_data: dict[str, pd.DataFrame] = execute_all_queries(
            mc_client,
            SQL_QUERIES,
            SQL_BASE_DIR,
            hints=MC_HINTS,
            params=default_params,
        )
        for name, df in mc_data.items():
            logger.info(
                f"  SQL query '{name}': {df.shape[0]} rows, {df.shape[1]} columns"
            )
    except Exception as e:
        logger.error(f"[Step 3/6] SQL execution failed: {e}")
        return 1

    # ------------------------------------------------------------------
    # 步骤 4: 数据转换（当前为 pass-through，逐 SQL 结果处理）
    # ------------------------------------------------------------------
    try:
        logger.info("[Step 4/6] Running transform pipeline...")
        transformer = build_cr_trail_transformer()
        # 逐个 SQL 结果进行转换，回写 mc_data 供路由引用
        for name in list(mc_data.keys()):
            mc_data[name] = transformer.transform(mc_data[name])
            logger.info(
                f"  Transform '{name}': {mc_data[name].shape[0]} rows, "
                f"{mc_data[name].shape[1]} columns"
            )
    except Exception as e:
        logger.error(f"[Step 4/6] Transform pipeline failed: {e}")
        return 1

    # ------------------------------------------------------------------
    # 步骤 5: 路由写入目标表
    # ------------------------------------------------------------------
    routes_had_failure = False
    try:
        if DATA_ROUTES:
            # 路由模式：per-route 日期作用域清理
            # 设计原理：每条路由根据自身源数据的日期范围独立计算清理窗口，
            # 不同 SQL 可能产出不同日期范围，共享窗口会导致误删
            effective_routes = []
            for route in DATA_ROUTES:
                # 从 source_ref 解析查询名称（如 "mc:conf_goods" → "conf_goods"）
                parts = route.source_ref.split(":", 1)
                if len(parts) == 2 and parts[0] == "mc" and parts[1] in mc_data:
                    source_df = mc_data[parts[1]]
                else:
                    source_df = pd.DataFrame()
                # 仅对该路由的源数据计算日期窗口
                effective_routes.extend(_apply_date_scoped_cleanup([route], source_df))
            router = DataRouter(lark_client, coercer, validator=SchemaValidator())
            logger.info(router.describe_routes(effective_routes))
            report = router.route(
                effective_routes,
                lark_data={},  # 无飞书源
                mc_data=mc_data,
                file_data=local_data,
                result_df=None,
            )
            logger.info(f"[Step 5/6] Route completed: {report.summary}")
        else:
            # 回退模式：直接写入 LARK_TARGETS
            from workers.lib import write_to_all_targets

            first_name = SQL_QUERIES[0].name
            logger.info(f"[Step 5/6] Writing to {len(LARK_TARGETS)} target table(s)...")
            write_to_all_targets(
                lark_client, mc_data[first_name], LARK_TARGETS, coercer=coercer
            )
    except Exception as e:
        logger.error(f"[Step 5/6] Target write had failures: {e}")
        routes_had_failure = True

    # ------------------------------------------------------------------
    # 步骤 6: 结果汇总
    # ------------------------------------------------------------------
    logger.info("[Step 6/6] Pipeline summary:")
    logger.info(f"  Local sources processed: {len(local_data)}")
    logger.info(f"  SQL queries executed:   {len(mc_data)}")
    for name, df in mc_data.items():
        logger.info(f"    - {name}: {df.shape[0]} rows, {df.shape[1]} columns")
    if DATA_ROUTES:
        logger.info(f"  Routes configured:      {len(DATA_ROUTES)}")
        for route in DATA_ROUTES:
            logger.info(f"    - {route.name} -> {route.target.table_name}")
    else:
        logger.info(f"  Target tables written:  {len(LARK_TARGETS)}")
    logger.info("=" * 60)
    if routes_had_failure:
        logger.warning("CR Trail Pipeline - COMPLETED WITH FAILURES")
        logger.info("=" * 60)
        return 1
    logger.info("CR Trail Pipeline - COMPLETED SUCCESSFULLY")
    logger.info("=" * 60)

    return 0


def main():
    """入口函数，供命令行调用

    用法：
        python -m workers.cr_trail.main
    """
    parser = argparse.ArgumentParser(description="CR试验 商品配置 ETL 管道")
    parser.parse_args()  # parse but ignore args; pipeline uses config.py
    sys.exit(run_cr_trail_pipeline())


if __name__ == "__main__":
    main()
