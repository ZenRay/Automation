# coding:utf8
"""workers.cr_analyze.main -- 管道编排 + CLI 入口

流程：
  1. 初始化 Lark + MaxCompute 客户端
    2. 提取 7 张飞书配置表
  3. 执行 MaxCompute SQL 查询
  4. 计算聚合宽表
  5. 写入 SQLite
  6. （可选）功效分析
"""

import argparse
import logging
import sys
from datetime import date as _date
from pathlib import Path

from automation.conf import lark as lark_conf, maxcomputer as mc_conf
from automation.client import LarkMultiDimTable, MaxComputerClient
from automation import hints as MC_HINTS

from workers.lib import extract_all_lark_sources, execute_all_queries
from workers.lib.lark_extractor import extract_single_source

from .config import (
    LARK_SOURCES,
    SQL_QUERIES,
    SQL_BASE_DIR,
    DEFAULT_DB_PATH,
)
from .sqlite_store import write_tables
from .transformer import (
    compute_wide_table,
    compute_trial_phase_config_wide,
    compute_trial_phase_config_pivot,
    compute_trial_sku_profile,
)

logger = logging.getLogger("workers.cr_analyze.main")


def _normalize_lark_date_columns(lark_data: dict) -> dict:
    """将 Lark 数据中日期/日期时间字段统一转为 date。"""
    import pandas as pd

    def _to_local_date(series: pd.Series) -> pd.Series:
        # Lark 日期列常以 UTC 时间戳落地为字符串（如 16:00:00），统一按上海时区取 date
        dt = pd.to_datetime(series, errors="coerce", utc=True)
        return dt.dt.tz_convert("Asia/Shanghai").dt.date

    normalized = {}
    for table_name, df in lark_data.items():
        if df is None or df.empty:
            normalized[table_name] = df
            continue

        df2 = df.copy()
        for col in df2.columns:
            col_name = str(col)
            is_date_name = col_name == "日期" or col_name.endswith("日期")
            is_datetime_dtype = pd.api.types.is_datetime64_any_dtype(df2[col])
            if is_date_name or is_datetime_dtype:
                df2[col] = _to_local_date(df2[col])

        normalized[table_name] = df2

    return normalized


def _normalize_lark_region_full_name_columns(lark_data: dict) -> dict:
    """标准化 Lark 数据中的区域全称，纠正省市顺序写反。"""
    import pandas as pd

    province_suffixes = ("省", "自治区", "特别行政区", "市")
    city_suffixes = ("市", "州", "地区", "盟")

    def _is_province_token(token: str) -> bool:
        token = str(token).strip()
        return any(token.endswith(s) for s in province_suffixes)

    def _is_city_token(token: str) -> bool:
        token = str(token).strip()
        return any(token.endswith(s) for s in city_suffixes)

    def _normalize_region_full_name(value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return value
        text = str(value).strip()
        if not text:
            return text

        # 仅纠正两段写反值：市-省 -> 省-市
        parts = [p.strip() for p in text.split("-") if p and str(p).strip()]
        if len(parts) == 2:
            left, right = parts[0], parts[1]
            if _is_city_token(left) and _is_province_token(right):
                return f"{right}-{left}"

        return text

    normalized = {}
    for table_name, df in lark_data.items():
        if df is None or df.empty:
            normalized[table_name] = df
            continue

        df2 = df.copy()
        if "区域全称" in df2.columns:
            before = df2["区域全称"].astype(str)
            df2["区域全称"] = df2["区域全称"].apply(_normalize_region_full_name)
            changed = (before != df2["区域全称"].astype(str)).sum()
            if changed:
                logger.info(f"  {table_name}: normalized 区域全称 rows={int(changed)}")

        normalized[table_name] = df2

    return normalized


def _init_lark_client() -> LarkMultiDimTable:
    app_id = lark_conf.get("prod", "APP_ID")
    app_secret = lark_conf.get("prod", "APP_SECRET")
    lark_host = lark_conf.get("prod", "LARK_HOST", fallback="https://open.feishu.cn")
    logger.info(f"Initializing Lark client (app_id={app_id})")
    return LarkMultiDimTable(app_id=app_id, app_secret=app_secret, lark_host=lark_host)


def _init_mc_client() -> MaxComputerClient:
    conf = {
        "access_id": mc_conf.get("prod", "access_id"),
        "secret_access_key": mc_conf.get("prod", "secret_access_key"),
        "project": mc_conf.get("prod", "project"),
        "endpoint": mc_conf.get("prod", "endpoint"),
    }
    logger.info(f"Initializing MaxCompute client (project={conf['project']})")
    return MaxComputerClient(**conf)


def run_cr_analyze_pipeline(
    target_date: _date | None = None,
    db_path: str | None = None,
    skip_mc: bool = False,
) -> int:
    """运行完整数据管道。

    Args:
        target_date: 目标日期，默认今天
        db_path:     SQLite 输出路径，默认 data/cr_analyze.db

    Returns:
        0 成功, 1 失败
    """
    if target_date is None:
        target_date = _date.today()
    if db_path is None:
        db_path = str(DEFAULT_DB_PATH)

    logger.info(f"Pipeline start: date={target_date}, db_path={db_path}")

    try:
        # 1. 初始化客户端
        lark_client = _init_lark_client()
        if not skip_mc:
            mc_client = _init_mc_client()

        # 2. 注入日期过滤 (conf_county_info 需要 target_date)
        for source in LARK_SOURCES:
            if source.name == "conf_county_info":
                source.date_filter_start_date = target_date
                source.date_filter_end_date = target_date

        # 3. 提取飞书数据 (含重试逻辑应对 InvalidPageToken)
        import time

        logger.info(f"Extracting {len(LARK_SOURCES)} Lark sources...")
        lark_data = {}
        for source in LARK_SOURCES:
            for attempt in range(3):
                try:
                    df = extract_single_source(lark_client, source)
                    lark_data[source.name] = df
                    logger.info(f"  {source.name}: {len(df)} rows")
                    break
                except Exception as e:
                    if attempt < 2:
                        logger.warning(
                            f"  {source.name}: {type(e).__name__} (attempt {attempt + 1}), retrying..."
                        )
                        time.sleep(3)
                    else:
                        raise

        # 3.1 应用层统一日期粒度: datetime -> date，避免后续跨源关联失败
        lark_data = _normalize_lark_date_columns(lark_data)
        # 3.2 应用层统一区域全称口径: 修正市-省写反
        lark_data = _normalize_lark_region_full_name_columns(lark_data)

        # 4. 执行 MaxCompute SQL（或从已有 SQLite 读取）
        if skip_mc:
            logger.info("Skipping MaxCompute (--skip-mc), reading fact from SQLite...")
            from .sqlite_store import read_table, table_exists

            if table_exists(db_path, "fact_order_item"):
                mc_data = {"fact_order_item": read_table(db_path, "fact_order_item")}
                logger.info(
                    f"  fact_order_item: {len(mc_data['fact_order_item'])} rows (from SQLite)"
                )
            else:
                logger.error(
                    "fact_order_item not in SQLite. Run without --skip-mc first."
                )
                return 1
        else:
            logger.info("Executing MaxCompute queries...")
            mc_data = execute_all_queries(
                mc_client, SQL_QUERIES, SQL_BASE_DIR, hints=MC_HINTS
            )
            for name, df in mc_data.items():
                logger.info(f"  {name}: {len(df)} rows")

        # 5. 计算聚合宽表
        logger.info("Computing aggregation wide table...")
        wide_df = compute_wide_table(lark_data, mc_data, target_date)
        logger.info(f"  agg_wide_table: {len(wide_df)} rows")

        logger.info("Computing trial phase config wide table...")
        phase_wide_df = compute_trial_phase_config_wide(lark_data)
        logger.info(f"  trial_phase_config_wide: {len(phase_wide_df)} rows")

        logger.info("Computing trial phase config pivot table...")
        phase_pivot_df = compute_trial_phase_config_pivot(phase_wide_df)
        logger.info(f"  trial_phase_config_pivot: {len(phase_pivot_df)} rows")

        logger.info("Computing trial SKU profile table...")
        trial_sku_profile_df = compute_trial_sku_profile(lark_data)
        logger.info(f"  trial_sku_profile: {len(trial_sku_profile_df)} rows")

        # 6. 组装所有表并写入 SQLite
        all_tables = {}
        all_tables.update(lark_data)
        all_tables.update(mc_data)
        all_tables["agg_wide_table"] = wide_df
        all_tables["trial_phase_config_wide"] = phase_wide_df
        all_tables["trial_phase_config_pivot"] = phase_pivot_df
        all_tables["trial_sku_profile"] = trial_sku_profile_df

        logger.info(f"Writing {len(all_tables)} tables to SQLite: {db_path}")
        count = write_tables(db_path, all_tables)
        logger.info(f"Pipeline complete. {count} tables written.")

        return 0

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        return 1


def run_power_analysis(db_path: str | None = None) -> int:
    """运行功效分析。

    从 SQLite 读取 fact_order_item 数据，计算 σ/ρ/功效，
    将结果写入 power_analysis 表。
    """
    from .config import DEFAULT_DB_PATH, TRIAL_PHASE_CONFIG
    from .sqlite_store import read_table, table_exists, write_tables
    from .transformer import compute_power_analysis

    if db_path is None:
        db_path = str(DEFAULT_DB_PATH)

    logger.info(f"Power analysis: reading from {db_path}")

    try:
        # 先尝试从 SQLite 读取已有的 fact 数据
        if not table_exists(db_path, "fact_order_item"):
            logger.info("fact_order_item not in SQLite, running pipeline first...")
            rc = run_cr_analyze_pipeline(db_path=db_path)
            if rc != 0:
                return rc

        fact_df = read_table(db_path, "fact_order_item")
        logger.info(f"Loaded fact_order_item: {len(fact_df)} rows")

        # P1-6: 使用 county_id -> 市名称 -> city_unit 映射，避免按 sku 聚合导致城市错配
        if table_exists(db_path, "conf_county_info"):
            import pandas as pd

            county_info = read_table(db_path, "conf_county_info")
            trial_group = (
                read_table(db_path, "conf_trial_group")
                if table_exists(db_path, "conf_trial_group")
                else pd.DataFrame()
            )

            if not county_info.empty and "区县id" in county_info.columns:
                county_map = county_info[["区县id", "市名称"]].copy()
                county_map["county_id"] = pd.to_numeric(
                    county_map["区县id"], errors="coerce"
                )
                county_map["市名称"] = (
                    county_map["市名称"].astype(str).replace("nan", pd.NA)
                )
                county_map = county_map.dropna(subset=["county_id", "市名称"])
                county_map["county_id"] = county_map["county_id"].astype(int)
                county_map = county_map[["county_id", "市名称"]].drop_duplicates(
                    subset=["county_id"]
                )

                if not trial_group.empty and "市名称" in trial_group.columns:
                    tg_city = trial_group[["市名称", "区域名称"]].copy()
                    tg_city["市名称"] = (
                        tg_city["市名称"].astype(str).replace("nan", pd.NA)
                    )
                    tg_city["city_unit"] = tg_city["市名称"].fillna(tg_city["区域名称"])
                    tg_city = tg_city.dropna(subset=["市名称", "city_unit"])
                    tg_city = tg_city[["市名称", "city_unit"]].drop_duplicates(
                        subset=["市名称"]
                    )
                    county_map = county_map.merge(tg_city, on="市名称", how="left")
                else:
                    county_map["city_unit"] = county_map["市名称"]

                if "区县id" in fact_df.columns:
                    fact_df["county_id"] = pd.to_numeric(
                        fact_df["区县id"], errors="coerce"
                    )
                elif "county_id" in fact_df.columns:
                    fact_df["county_id"] = pd.to_numeric(
                        fact_df["county_id"], errors="coerce"
                    )

                fact_df = fact_df.drop(columns=["city_unit"], errors="ignore")
                fact_df = fact_df.merge(
                    county_map[["county_id", "city_unit"]],
                    on="county_id",
                    how="left",
                )

        # 确保日期列为 date 类型
        import pandas as pd

        if "日期" in fact_df.columns:
            fact_df["日期"] = pd.to_datetime(fact_df["日期"], errors="coerce").dt.date

        # 重命名列以匹配 power analysis 期望
        from .config import FIELD_MAPPING

        rename_map = {k: v for k, v in FIELD_MAPPING.items() if k in fact_df.columns}
        fact_df = fact_df.rename(columns=rename_map)

        # 计算功效
        logger.info("Computing power analysis...")
        result = compute_power_analysis(fact_df, TRIAL_PHASE_CONFIG)

        if result.empty:
            logger.warning("Power analysis produced empty result")
            return 1

        # 写入 SQLite
        cross_corr = result.attrs.get("cross_correlation", [])
        cross_df = (
            pd.DataFrame(cross_corr)
            if cross_corr
            else pd.DataFrame(columns=["sku_a", "sku_b", "rho", "risk_flag"])
        )
        if not cross_df.empty and "rho" in cross_df.columns:
            cross_df["risk_flag"] = cross_df["rho"].apply(
                lambda x: bool(pd.notna(x) and x > 0.5)
            )

        write_tables(db_path, {"power_analysis": result, "power_cross_correlation": cross_df})
        logger.info(
            f"Power analysis results written to power_analysis table ({len(result)} rows)"
        )

        # 打印摘要
        for _, row in result.iterrows():
            sku = row["sku_id"]
            sigma = row.get("sigma_raw", float("nan"))
            rho = row.get("rho_main", float("nan"))
            n_req = row.get("n_required", float("nan"))
            sufficient = row.get("power_sufficient", False)
            conclusion = "功效充足 ✅" if sufficient else "功效不足 ❌"
            logger.info(
                f"  SKU {sku}: σ_raw={sigma:.4f}, ρ_main={rho:.4f}, "
                f"n_required={n_req:.2f} → {conclusion}"
            )

        # 交叉相关
        cross_corr = result.attrs.get("cross_correlation", [])
        if cross_corr:
            logger.info("SKU cross-correlation:")
            for pair in cross_corr:
                flagged = " ⚠️ > 0.5" if pair.get("rho", 0) > 0.5 else ""
                logger.info(
                    f"  ({pair['sku_a']}, {pair['sku_b']}): ρ={pair.get('rho', float('nan')):.4f}{flagged}"
                )

        return 0

    except Exception as e:
        logger.error(f"Power analysis failed: {e}", exc_info=True)
        return 1


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(message)s",
    )

    parser = argparse.ArgumentParser(description="抽佣率试验分析数据管道")
    parser.add_argument(
        "--date", type=str, default=None, help="目标日期 (YYYY-MM-DD)，默认今天"
    )
    parser.add_argument("--db-path", type=str, default=None, help="SQLite 输出路径")
    parser.add_argument("--power", action="store_true", help="运行功效分析模式")
    parser.add_argument(
        "--skip-mc",
        action="store_true",
        help="跳过 MaxCompute SQL，从已有 SQLite 读取 fact 数据",
    )
    args = parser.parse_args()

    target_date = None
    if args.date:
        target_date = _date.fromisoformat(args.date)

    if args.power:
        logger.info("Power analysis mode")
        rc = run_power_analysis(db_path=args.db_path)
        sys.exit(rc)

    rc = run_cr_analyze_pipeline(
        target_date=target_date, db_path=args.db_path, skip_mc=args.skip_mc
    )
    sys.exit(rc)


if __name__ == "__main__":
    main()
