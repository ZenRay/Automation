# coding:utf8
"""workers.upgrade_after_sale.main -- 售后/订单明细同步飞书

能力：
1. 两条 SQL 独立 offset 参数执行
2. 两张目标表自动创建（不存在时）
3. 每条路由按自身日期窗口清理后写入
4. 售后附件字段走统一附件上传链路
5. route 级 row_key 持久化与失败重试
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import shutil
import sys
import time
from datetime import date as _date, timedelta as _timedelta
from pathlib import Path

import pandas as pd

from automation import hints as MC_HINTS
from automation.client import LarkMultiDimTable, MaxComputerClient
from automation.conf import lark as lark_conf, maxcomputer as mc_conf

from workers.lib import (
    AttachmentTokenResolver,
    DataRouter,
    DateRangeParams,
    FieldTypeCoercer,
    PersistenceConfig,
    SchemaValidator,
    execute_all_queries,
)
from workers.lib.models import CleanupCondition, DataRoute
from .config import (
    ATTACHMENT_MAX_SIZE_MB,
    ATTACHMENT_BAK_SOURCE_FIELDS,
    ATTACHMENT_BAK_SUFFIX,
    DATA_ROUTES,
    ENABLE_ATTACHMENT_BAK,
    QUERY_WINDOWS,
    RETRYABLE_ERROR_PATTERNS,
    ROUTE_RETRY_BACKOFF_MULTIPLIER,
    ROUTE_RETRY_BACKOFF_SECONDS,
    ROUTE_RETRY_MAX_ATTEMPTS,
    ROUTE_DATE_FIELDS,
    SQL_BASE_DIR,
    SQL_QUERIES,
)
from .transformer import (
    normalize_after_sale_df,
    normalize_order_item_df,
    normalize_store_stat_df,
    normalize_store_cat1_stat_df,
    normalize_cat4_stat_df,
    normalize_mct_cat4_stat_df,
    normalize_sku_stat_df,
)

logger = logging.getLogger("workers.upgrade_after_sale.main")

DEFAULT_PERSISTENCE_ROOT = Path("logs") / "persistence" / "upgrade_after_sale"
PERSISTENCE_RETENTION_DAYS = 4


def _init_lark_client() -> LarkMultiDimTable:
    app_id = lark_conf.get("prod", "APP_ID")
    app_secret = lark_conf.get("prod", "APP_SECRET")
    lark_host = lark_conf.get("prod", "LARK_HOST", fallback="https://open.feishu.cn")
    logger.info("Initializing Lark client (app_id=%s, host=%s)", app_id, lark_host)
    return LarkMultiDimTable(app_id=app_id, app_secret=app_secret, lark_host=lark_host)


def _init_mc_client() -> MaxComputerClient:
    conf = {
        "access_id": mc_conf.get("prod", "access_id"),
        "secret_access_key": mc_conf.get("prod", "secret_access_key"),
        "project": mc_conf.get("prod", "project"),
        "endpoint": mc_conf.get("prod", "endpoint"),
    }
    logger.info("Initializing MaxComputer client (project=%s)", conf["project"])
    return MaxComputerClient(**conf)


def _build_date_params(date_value: str | None, start: int, end: int) -> DateRangeParams:
    ref_date = _date.fromisoformat(date_value) if date_value else None
    return DateRangeParams(
        start_offset=start,
        end_offset=end,
        date_param=f"DATE '{ref_date.isoformat()}'" if ref_date else "CURRENT_DATE()",
        reference_date=ref_date,
    )


def _compute_window(date_param: DateRangeParams) -> tuple[_date, _date]:
    ref = date_param.reference_date or _date.today()
    return (
        ref + _timedelta(days=date_param.start_offset),
        ref + _timedelta(days=date_param.end_offset),
    )


def _validate_offsets(name: str, start: int, end: int) -> None:
    if start > end:
        raise ValueError(
            f"Invalid offset window for '{name}': start={start} > end={end}. "
            "Expected start_offset <= end_offset."
        )


def _replace_cleanup_windows(
    routes: list[DataRoute],
    route_windows: dict[str, tuple[_date, _date]],
) -> list[DataRoute]:
    """按 route 级窗口替换 runtime_window 哨兵。"""
    replaced = []
    for route in routes:
        target = route.target
        if (
            target.cleanup_conditions is None
            or not target.cleanup_conditions.is_runtime
        ):
            replaced.append(route)
            continue

        start_date, end_date = route_windows[route.name]
        date_field = ROUTE_DATE_FIELDS.get(route.name, "日期")
        new_cleanup = CleanupCondition.date_window(date_field, start_date, end_date)
        new_target = dataclasses.replace(target, cleanup_conditions=new_cleanup)
        replaced.append(dataclasses.replace(route, target=new_target))
    return replaced


def _build_attachment_resolver(
    lark_client: LarkMultiDimTable,
) -> AttachmentTokenResolver:
    """附件解析器：由 lark_loader 在写入阶段注入到 coercer。"""
    return AttachmentTokenResolver(
        client=lark_client,
        app_token=None,
        max_size_mb=ATTACHMENT_MAX_SIZE_MB,
        max_retries=2,
        backoff_seconds=0.4,
    )


def _build_row_key(df: pd.DataFrame, route_name: str) -> pd.Series:
    if route_name == "after_sale_detail":
        key_col = "售后单id"
    elif route_name == "order_detail":
        key_col = "明细订单id"
    elif route_name == "store_stat_detail":
        # 复合键：店铺id + 日期
        for col in ("店铺id", "日期"):
            if col not in df.columns:
                raise ValueError(
                    f"Missing row key column '{col}' for route '{route_name}'"
                )
        row_key = df["店铺id"].astype(str) + "_" + df["日期"].astype(str)
        if row_key.eq("").any() or row_key.str.startswith("nan_").any():
            raise ValueError(f"Empty row_key detected for route '{route_name}'")
        return row_key
    elif route_name == "store_cat1_stat_detail":
        # 三级复合键：店铺id + 一级类目id + 日期
        for col in ("店铺id", "一级类目id", "日期"):
            if col not in df.columns:
                raise ValueError(
                    f"Missing row key column '{col}' for route '{route_name}'"
                )
        row_key = (
            df["店铺id"].astype(str)
            + "_"
            + df["一级类目id"].astype(str)
            + "_"
            + df["日期"].astype(str)
        )
        if row_key.eq("").any() or row_key.str.startswith("nan_").any():
            raise ValueError(f"Empty row_key detected for route '{route_name}'")
        return row_key
    elif route_name == "cat4_stat_detail":
        # 三级复合键：一级类目id + 四级类目id + 日期
        for col in ("一级类目id", "四级类目id", "日期"):
            if col not in df.columns:
                raise ValueError(
                    f"Missing row key column '{col}' for route '{route_name}'"
                )
        row_key = (
            df["一级类目id"].astype(str)
            + "_"
            + df["四级类目id"].astype(str)
            + "_"
            + df["日期"].astype(str)
        )
        if row_key.eq("").any() or row_key.str.startswith("nan_").any():
            raise ValueError(f"Empty row_key detected for route '{route_name}'")
        return row_key
    elif route_name == "mct_cat4_stat_detail":
        # 四级复合键：商家id + 一级类目id + 四级类目id + 日期
        for col in ("商家id", "一级类目id", "四级类目id", "日期"):
            if col not in df.columns:
                raise ValueError(
                    f"Missing row key column '{col}' for route '{route_name}'"
                )
        row_key = (
            df["商家id"].astype(str)
            + "_"
            + df["一级类目id"].astype(str)
            + "_"
            + df["四级类目id"].astype(str)
            + "_"
            + df["日期"].astype(str)
        )
        if row_key.eq("").any() or row_key.str.startswith("nan_").any():
            raise ValueError(f"Empty row_key detected for route '{route_name}'")
        return row_key
    elif route_name == "sku_stat_detail":
        # 五级复合键：商家id + 一级类目id + 四级类目id + 商品id + 日期
        for col in ("商家id", "一级类目id", "四级类目id", "商品id", "日期"):
            if col not in df.columns:
                raise ValueError(
                    f"Missing row key column '{col}' for route '{route_name}'"
                )
        row_key = (
            df["商家id"].astype(str)
            + "_"
            + df["一级类目id"].astype(str)
            + "_"
            + df["四级类目id"].astype(str)
            + "_"
            + df["商品id"].astype(str)
            + "_"
            + df["日期"].astype(str)
        )
        if row_key.eq("").any() or row_key.str.startswith("nan_").any():
            raise ValueError(f"Empty row_key detected for route '{route_name}'")
        return row_key
    else:
        raise ValueError(f"Unsupported route_name for row_key: {route_name}")

    if key_col not in df.columns:
        raise ValueError(f"Missing row key column '{key_col}' for route '{route_name}'")

    row_key = df[key_col].astype(str)
    if row_key.eq("").any():
        raise ValueError(f"Empty row_key detected for route '{route_name}'")
    return row_key


def _inject_row_key(mc_data: dict[str, pd.DataFrame]) -> None:
    route_to_source = {
        "after_sale_detail": "after_sale_item",
        "order_detail": "order_item",
        "store_stat_detail": "store_stat",
        "store_cat1_stat_detail": "store_cat1_stat",
        "cat4_stat_detail": "cat4_stat",
        "mct_cat4_stat_detail": "mct_cat4_stat",
        "sku_stat_detail": "sku_stat",
    }
    for route_name, source_name in route_to_source.items():
        if source_name not in mc_data:
            continue
        df = mc_data[source_name]
        mc_data[source_name] = df.assign(row_key=_build_row_key(df, route_name))


def _cleanup_old_persistence_dirs(
    base_dir: Path, retention_days: int = PERSISTENCE_RETENTION_DAYS
) -> None:
    if not base_dir.exists():
        return

    now_ts = time.time()
    retention_seconds = retention_days * 24 * 60 * 60
    for child in base_dir.iterdir():
        if not child.is_dir():
            continue
        try:
            age_seconds = now_ts - child.stat().st_mtime
            if age_seconds > retention_seconds:
                shutil.rmtree(child)
                logger.info("Removed old persistence directory: %s", child)
        except Exception as exc:
            logger.warning("Failed to cleanup persistence directory %s: %s", child, exc)


def _build_persistence_config(
    *,
    enabled: bool,
    persistence_dir: str | None,
    job_id: str | None,
    retry_failed_only: bool,
) -> PersistenceConfig:
    base_dir = Path(persistence_dir) if persistence_dir else DEFAULT_PERSISTENCE_ROOT
    base_dir.mkdir(parents=True, exist_ok=True)
    _cleanup_old_persistence_dirs(base_dir)

    if job_id:
        effective_job_id = job_id
    else:
        effective_job_id = _date.today().isoformat()

    config = PersistenceConfig(
        enabled=enabled,
        artifact_dir=str(base_dir),
        job_id=effective_job_id,
        retry_failed_only=retry_failed_only,
    )
    if enabled:
        logger.info(
            "Persistence enabled: dir=%s, job_id=%s, retry_failed_only=%s",
            config.artifact_dir,
            config.job_id,
            config.retry_failed_only,
        )
    return config


def _is_retryable_error(exc: Exception) -> bool:
    text = str(exc).lower()
    return any(pattern in text for pattern in RETRYABLE_ERROR_PATTERNS)


def _apply_attachment_bak_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not ENABLE_ATTACHMENT_BAK:
        return df
    if not ATTACHMENT_BAK_SOURCE_FIELDS:
        return df

    work_df = df.copy()
    for source_col in ATTACHMENT_BAK_SOURCE_FIELDS:
        if source_col not in work_df.columns:
            continue
        bak_col = f"{source_col}{ATTACHMENT_BAK_SUFFIX}"
        work_df[bak_col] = work_df[source_col].astype(str)
        work_df.loc[work_df[source_col].isna(), bak_col] = ""
        # 保留原附件字段用于正常上传，bak 仅用于失败/超限时追溯原值。
    return work_df


def _route_with_retry(
    router: DataRouter,
    routes: list[DataRoute],
    *,
    lark_data: dict[str, pd.DataFrame],
    mc_data: dict[str, pd.DataFrame],
    file_data: dict[str, pd.DataFrame],
) -> object:
    attempt = 1
    while True:
        try:
            report = router.route(
                routes,
                lark_data=lark_data,
                mc_data=mc_data,
                file_data=file_data,
                result_df=None,
            )
            if attempt == 1:
                logger.info(
                    "[Step 4/5] Route completed on first attempt: %s", report.summary
                )
            else:
                logger.info(
                    "[Step 4/5] Route completed after retry (attempt=%s): %s",
                    attempt,
                    report.summary,
                )
            return report
        except Exception as exc:
            retryable = _is_retryable_error(exc)
            if not retryable or attempt >= ROUTE_RETRY_MAX_ATTEMPTS:
                raise
            wait_seconds = ROUTE_RETRY_BACKOFF_SECONDS * (
                ROUTE_RETRY_BACKOFF_MULTIPLIER ** (attempt - 1)
            )
            logger.warning(
                "[Step 4/5] Route attempt %s/%s failed (retryable=%s): %s; retry in %.2fs",
                attempt,
                ROUTE_RETRY_MAX_ATTEMPTS,
                retryable,
                exc,
                wait_seconds,
            )
            time.sleep(wait_seconds)
            attempt += 1


def run_upgrade_after_sale_pipeline(
    *,
    date_value: str | None = None,
    after_sale_start: int | None = None,
    after_sale_end: int | None = None,
    order_start: int | None = None,
    order_end: int | None = None,
    store_stat_start: int | None = None,
    store_stat_end: int | None = None,
    store_cat1_stat_start: int | None = None,
    store_cat1_stat_end: int | None = None,
    cat4_stat_start: int | None = None,
    cat4_stat_end: int | None = None,
    mct_cat4_stat_start: int | None = None,
    mct_cat4_stat_end: int | None = None,
    sku_stat_start: int | None = None,
    sku_stat_end: int | None = None,
    enable_persistence: bool = False,
    persistence_dir: str | None = None,
    job_id: str | None = None,
    retry_failed_only: bool = False,
) -> int:
    logger.info("=" * 60)
    logger.info("Upgrade After Sale Pipeline - START")
    logger.info("=" * 60)

    try:
        logger.info("[Step 1/5] Initializing clients...")
        lark_client = _init_lark_client()
        mc_client = _init_mc_client()
        attachment_resolver = _build_attachment_resolver(lark_client)
        coercer = FieldTypeCoercer(
            attachment_resolver=attachment_resolver.resolve_single
        )
    except Exception as e:
        logger.error("[Step 1/5] Client initialization failed: %s", e)
        return 1

    # 四条 SQL 独立窗口
    as_start = (
        after_sale_start
        if after_sale_start is not None
        else QUERY_WINDOWS["after_sale_item"]["start"]
    )
    as_end = (
        after_sale_end
        if after_sale_end is not None
        else QUERY_WINDOWS["after_sale_item"]["end"]
    )
    od_start = (
        order_start if order_start is not None else QUERY_WINDOWS["order_item"]["start"]
    )
    od_end = order_end if order_end is not None else QUERY_WINDOWS["order_item"]["end"]
    ss_start = (
        store_stat_start
        if store_stat_start is not None
        else QUERY_WINDOWS["store_stat"]["start"]
    )
    ss_end = (
        store_stat_end
        if store_stat_end is not None
        else QUERY_WINDOWS["store_stat"]["end"]
    )
    sc1_start = (
        store_cat1_stat_start
        if store_cat1_stat_start is not None
        else QUERY_WINDOWS["store_cat1_stat"]["start"]
    )
    sc1_end = (
        store_cat1_stat_end
        if store_cat1_stat_end is not None
        else QUERY_WINDOWS["store_cat1_stat"]["end"]
    )
    c4_start = (
        cat4_stat_start
        if cat4_stat_start is not None
        else QUERY_WINDOWS["cat4_stat"]["start"]
    )
    c4_end = (
        cat4_stat_end
        if cat4_stat_end is not None
        else QUERY_WINDOWS["cat4_stat"]["end"]
    )
    mc4_start = (
        mct_cat4_stat_start
        if mct_cat4_stat_start is not None
        else QUERY_WINDOWS["mct_cat4_stat"]["start"]
    )
    mc4_end = (
        mct_cat4_stat_end
        if mct_cat4_stat_end is not None
        else QUERY_WINDOWS["mct_cat4_stat"]["end"]
    )
    sk_start = (
        sku_stat_start
        if sku_stat_start is not None
        else QUERY_WINDOWS["sku_stat"]["start"]
    )
    sk_end = (
        sku_stat_end if sku_stat_end is not None else QUERY_WINDOWS["sku_stat"]["end"]
    )

    try:
        _validate_offsets("after_sale_item", as_start, as_end)
        _validate_offsets("order_item", od_start, od_end)
        _validate_offsets("store_stat", ss_start, ss_end)
        _validate_offsets("store_cat1_stat", sc1_start, sc1_end)
        _validate_offsets("cat4_stat", c4_start, c4_end)
        _validate_offsets("mct_cat4_stat", mc4_start, mc4_end)
        _validate_offsets("sku_stat", sk_start, sk_end)
    except ValueError as e:
        logger.error("[Step 1/5] Offset validation failed: %s", e)
        return 1

    as_params = _build_date_params(date_value, as_start, as_end)
    od_params = _build_date_params(date_value, od_start, od_end)
    ss_params = _build_date_params(date_value, ss_start, ss_end)
    sc1_params = _build_date_params(date_value, sc1_start, sc1_end)
    c4_params = _build_date_params(date_value, c4_start, c4_end)
    mc4_params = _build_date_params(date_value, mc4_start, mc4_end)
    sk_params = _build_date_params(date_value, sk_start, sk_end)

    as_window = _compute_window(as_params)
    od_window = _compute_window(od_params)
    ss_window = _compute_window(ss_params)
    sc1_window = _compute_window(sc1_params)
    c4_window = _compute_window(c4_params)
    mc4_window = _compute_window(mc4_params)
    sk_window = _compute_window(sk_params)

    logger.info(
        "after_sale_item params: date_param=%s, start=%s, end=%s, window=%s~%s",
        as_params.sql_params()["date_param"],
        as_start,
        as_end,
        as_window[0],
        as_window[1],
    )
    logger.info(
        "order_item params: date_param=%s, start=%s, end=%s, window=%s~%s",
        od_params.sql_params()["date_param"],
        od_start,
        od_end,
        od_window[0],
        od_window[1],
    )
    logger.info(
        "store_stat params: date_param=%s, start=%s, end=%s, window=%s~%s",
        ss_params.sql_params()["date_param"],
        ss_start,
        ss_end,
        ss_window[0],
        ss_window[1],
    )
    logger.info(
        "store_cat1_stat params: date_param=%s, start=%s, end=%s, window=%s~%s",
        sc1_params.sql_params()["date_param"],
        sc1_start,
        sc1_end,
        sc1_window[0],
        sc1_window[1],
    )
    logger.info(
        "cat4_stat params: date_param=%s, start=%s, end=%s, window=%s~%s",
        c4_params.sql_params()["date_param"],
        c4_start,
        c4_end,
        c4_window[0],
        c4_window[1],
    )
    logger.info(
        "mct_cat4_stat params: date_param=%s, start=%s, end=%s, window=%s~%s",
        mc4_params.sql_params()["date_param"],
        mc4_start,
        mc4_end,
        mc4_window[0],
        mc4_window[1],
    )
    logger.info(
        "sku_stat params: date_param=%s, start=%s, end=%s, window=%s~%s",
        sk_params.sql_params()["date_param"],
        sk_start,
        sk_end,
        sk_window[0],
        sk_window[1],
    )

    try:
        logger.info("[Step 2/5] Executing SQL queries with independent windows...")
        query_map = {q.name: q for q in SQL_QUERIES}
        after_sale_data = execute_all_queries(
            mc_client,
            [query_map["after_sale_item"]],
            SQL_BASE_DIR,
            hints=MC_HINTS,
            params=as_params.sql_params(),
        )
        order_data = execute_all_queries(
            mc_client,
            [query_map["order_item"]],
            SQL_BASE_DIR,
            hints=MC_HINTS,
            params=od_params.sql_params(),
        )
        store_stat_data = execute_all_queries(
            mc_client,
            [query_map["store_stat"]],
            SQL_BASE_DIR,
            hints=MC_HINTS,
            params=ss_params.sql_params(),
        )
        store_cat1_stat_data = execute_all_queries(
            mc_client,
            [query_map["store_cat1_stat"]],
            SQL_BASE_DIR,
            hints=MC_HINTS,
            params=sc1_params.sql_params(),
        )
        cat4_stat_data = execute_all_queries(
            mc_client,
            [query_map["cat4_stat"]],
            SQL_BASE_DIR,
            hints=MC_HINTS,
            params=c4_params.sql_params(),
        )
        mct_cat4_stat_data = execute_all_queries(
            mc_client,
            [query_map["mct_cat4_stat"]],
            SQL_BASE_DIR,
            hints=MC_HINTS,
            params=mc4_params.sql_params(),
        )
        sku_stat_data = execute_all_queries(
            mc_client,
            [query_map["sku_stat"]],
            SQL_BASE_DIR,
            hints=MC_HINTS,
            params=sk_params.sql_params(),
        )
        mc_data: dict[str, pd.DataFrame] = {}
        mc_data.update(after_sale_data)
        mc_data.update(order_data)
        mc_data.update(store_stat_data)
        mc_data.update(store_cat1_stat_data)
        mc_data.update(cat4_stat_data)
        mc_data.update(mct_cat4_stat_data)
        mc_data.update(sku_stat_data)
        logger.info(
            "SQL results: after_sale_item=%s rows, order_item=%s rows, "
            "store_stat=%s rows, store_cat1_stat=%s rows, "
            "cat4_stat=%s rows, mct_cat4_stat=%s rows, sku_stat=%s rows",
            len(mc_data.get("after_sale_item", pd.DataFrame())),
            len(mc_data.get("order_item", pd.DataFrame())),
            len(mc_data.get("store_stat", pd.DataFrame())),
            len(mc_data.get("store_cat1_stat", pd.DataFrame())),
            len(mc_data.get("cat4_stat", pd.DataFrame())),
            len(mc_data.get("mct_cat4_stat", pd.DataFrame())),
            len(mc_data.get("sku_stat", pd.DataFrame())),
        )
    except Exception as e:
        logger.error("[Step 2/5] SQL execution failed: %s", e)
        return 1

    try:
        logger.info("[Step 3/5] Running lightweight normalization...")
        if "after_sale_item" in mc_data:
            mc_data["after_sale_item"] = normalize_after_sale_df(
                mc_data["after_sale_item"]
            )
            mc_data["after_sale_item"] = _apply_attachment_bak_columns(
                mc_data["after_sale_item"]
            )
        if "order_item" in mc_data:
            mc_data["order_item"] = normalize_order_item_df(mc_data["order_item"])
        if "store_stat" in mc_data:
            mc_data["store_stat"] = normalize_store_stat_df(mc_data["store_stat"])
        if "store_cat1_stat" in mc_data:
            mc_data["store_cat1_stat"] = normalize_store_cat1_stat_df(
                mc_data["store_cat1_stat"]
            )
        if "cat4_stat" in mc_data:
            mc_data["cat4_stat"] = normalize_cat4_stat_df(mc_data["cat4_stat"])
        if "mct_cat4_stat" in mc_data:
            mc_data["mct_cat4_stat"] = normalize_mct_cat4_stat_df(
                mc_data["mct_cat4_stat"]
            )
        if "sku_stat" in mc_data:
            mc_data["sku_stat"] = normalize_sku_stat_df(mc_data["sku_stat"])
        _inject_row_key(mc_data)
    except Exception as e:
        logger.error("[Step 3/5] Normalization failed: %s", e)
        return 1

    try:
        logger.info("[Step 4/5] Routing to Feishu targets...")
        effective_routes = _replace_cleanup_windows(
            DATA_ROUTES,
            {
                "after_sale_detail": as_window,
                "order_detail": od_window,
                "store_stat_detail": ss_window,
                "store_cat1_stat_detail": sc1_window,
                "cat4_stat_detail": c4_window,
                "mct_cat4_stat_detail": mc4_window,
                "sku_stat_detail": sk_window,
            },
        )
        router = DataRouter(
            lark_client,
            coercer,
            validator=SchemaValidator(),
            attachment_resolver=attachment_resolver,
            persistence_config=_build_persistence_config(
                enabled=enable_persistence,
                persistence_dir=persistence_dir,
                job_id=job_id,
                retry_failed_only=retry_failed_only,
            ),
        )
        logger.info(router.describe_routes(effective_routes))
        report = _route_with_retry(
            router,
            effective_routes,
            lark_data={},
            mc_data=mc_data,
            file_data={},
        )

        route_to_source = {
            "after_sale_detail": "after_sale_item",
            "order_detail": "order_item",
            "store_stat_detail": "store_stat",
            "store_cat1_stat_detail": "store_cat1_stat",
            "cat4_stat_detail": "cat4_stat",
            "mct_cat4_stat_detail": "mct_cat4_stat",
            "sku_stat_detail": "sku_stat",
        }
        logger.info("[Step 4/5] Upload reconciliation (sql_rows vs uploaded_rows):")
        total_sql_rows = 0
        total_uploaded_rows = 0
        for result in report.results:
            source_name = route_to_source.get(result.route_name, "")
            sql_rows = (
                len(mc_data.get(source_name, pd.DataFrame()))
                if source_name
                else result.source_shape[0]
            )
            uploaded_rows = result.written_count
            delta = sql_rows - uploaded_rows
            total_sql_rows += sql_rows
            total_uploaded_rows += uploaded_rows
            logger.info(
                "  - route=%s source=%s sql_rows=%s uploaded_rows=%s delta=%s",
                result.route_name,
                source_name or result.source_ref,
                sql_rows,
                uploaded_rows,
                delta,
            )
        logger.info(
            "[Step 4/5] Upload reconciliation total: total_sql_rows=%s total_uploaded_rows=%s total_delta=%s",
            total_sql_rows,
            total_uploaded_rows,
            total_sql_rows - total_uploaded_rows,
        )
    except Exception as e:
        logger.error("[Step 4/5] Target write failed: %s", e)
        return 1

    logger.info("[Step 5/5] Pipeline summary:")
    for name, df in mc_data.items():
        logger.info("  - %s: %s rows, %s columns", name, df.shape[0], df.shape[1])
    logger.info("Upgrade After Sale Pipeline - COMPLETED SUCCESSFULLY")
    logger.info("=" * 60)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Upgrade After Sale pipeline")
    parser.add_argument("--date", type=str, default=None, help="基准日期 YYYY-MM-DD")
    parser.add_argument(
        "--as-start", type=int, default=None, help="售后 SQL start offset"
    )
    parser.add_argument("--as-end", type=int, default=None, help="售后 SQL end offset")
    parser.add_argument(
        "--order-start", type=int, default=None, help="订单 SQL start offset"
    )
    parser.add_argument(
        "--order-end", type=int, default=None, help="订单 SQL end offset"
    )
    parser.add_argument(
        "--store-stat-start",
        type=int,
        default=None,
        help="门店统计 SQL start offset",
    )
    parser.add_argument(
        "--store-stat-end",
        type=int,
        default=None,
        help="门店统计 SQL end offset",
    )
    parser.add_argument(
        "--store-cat1-stat-start",
        type=int,
        default=None,
        help="门店一级类目统计 SQL start offset",
    )
    parser.add_argument(
        "--store-cat1-stat-end",
        type=int,
        default=None,
        help="门店一级类目统计 SQL end offset",
    )
    parser.add_argument(
        "--cat4-stat-start",
        type=int,
        default=None,
        help="四级类目统计 SQL start offset",
    )
    parser.add_argument(
        "--cat4-stat-end",
        type=int,
        default=None,
        help="四级类目统计 SQL end offset",
    )
    parser.add_argument(
        "--mct-cat4-stat-start",
        type=int,
        default=None,
        help="商家四级类目统计 SQL start offset",
    )
    parser.add_argument(
        "--mct-cat4-stat-end",
        type=int,
        default=None,
        help="商家四级类目统计 SQL end offset",
    )
    parser.add_argument(
        "--sku-stat-start",
        type=int,
        default=None,
        help="商品统计 SQL start offset",
    )
    parser.add_argument(
        "--sku-stat-end",
        type=int,
        default=None,
        help="商品统计 SQL end offset",
    )
    parser.add_argument(
        "--enable-persistence", action="store_true", help="启用 route 写入持久化"
    )
    parser.add_argument(
        "--persistence-dir", type=str, default=None, help="持久化根目录"
    )
    parser.add_argument("--job-id", type=str, default=None, help="持久化 job_id")
    parser.add_argument(
        "--retry-failed-only", action="store_true", help="仅重试当前失败 row_key"
    )
    args = parser.parse_args()

    code = run_upgrade_after_sale_pipeline(
        date_value=args.date,
        after_sale_start=args.as_start,
        after_sale_end=args.as_end,
        order_start=args.order_start,
        order_end=args.order_end,
        store_stat_start=args.store_stat_start,
        store_stat_end=args.store_stat_end,
        store_cat1_stat_start=args.store_cat1_stat_start,
        store_cat1_stat_end=args.store_cat1_stat_end,
        cat4_stat_start=args.cat4_stat_start,
        cat4_stat_end=args.cat4_stat_end,
        mct_cat4_stat_start=args.mct_cat4_stat_start,
        mct_cat4_stat_end=args.mct_cat4_stat_end,
        sku_stat_start=args.sku_stat_start,
        sku_stat_end=args.sku_stat_end,
        enable_persistence=args.enable_persistence,
        persistence_dir=args.persistence_dir,
        job_id=args.job_id,
        retry_failed_only=args.retry_failed_only,
    )
    sys.exit(code)


if __name__ == "__main__":
    main()
