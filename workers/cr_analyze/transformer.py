# coding:utf8
"""workers.cr_analyze.transformer -- 聚合宽表计算 + 功效分析

核心函数 compute_wide_table 将飞书配置数据与 MaxCompute 事实数据
进行关联、过滤、聚合，生成核心宽表。
"""

import logging
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from .config import (
    FIELD_MAPPING,
    TARGET_SKU_IDS,
    TRIAL_PHASE_CONFIG,
)

logger = logging.getLogger("workers.cr_analyze.transformer")


def preprocess_lark_dates(
    lark_data: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """将飞书数据中的日期列统一转为 datetime.date 类型。"""
    result = {}
    for name, df in lark_data.items():
        df = df.copy()
        for col in df.columns:
            if col == "日期" or col.endswith("日期"):
                df[col] = _to_date_series(df[col])
        result[name] = df
    return result


def _to_date_series(series: pd.Series) -> pd.Series:
    """将任意日期/日期时间列统一按上海时区转为 datetime.date。"""
    dt = pd.to_datetime(series, errors="coerce", utc=True)
    return dt.dt.tz_convert("Asia/Shanghai").dt.date


def _normalize_city_name(series: pd.Series) -> pd.Series:
    """统一城市名：去除空白字符，避免 `株洲 市` 之类格式差异导致关联失败。"""
    return series.astype(str).str.replace(r"\s+", "", regex=True).replace("nan", pd.NA)


def compute_trial_phase_config_wide(
    lark_data: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """构建试验阶段配置宽表。

    规则:
      - 以 conf_trial_group 与 conf_trial_period_rate 做 FULL JOIN
      - JOIN 键: 试验分组 + 试验起始日期 + 试验结束日期
      - 日期统一转 date，供看板阶段/分组配置共用
    """
    trial_group_df = lark_data.get("conf_trial_group", pd.DataFrame()).copy()
    period_rate_df = lark_data.get("conf_trial_period_rate", pd.DataFrame()).copy()

    cols = [
        "市名称",
        "区域名称",
        "区域类型",
        "试验分组",
        "试验阶段",
        "试验起始日期",
        "试验结束日期",
        "运营类型",
        "抽佣率",
        "city_unit",
    ]

    if trial_group_df.empty and period_rate_df.empty:
        return pd.DataFrame(columns=cols)

    tg_keep = [
        c
        for c in ["市名称", "区域名称", "区域类型", "试验分组", "试验起始日期", "试验结束日期"]
        if c in trial_group_df.columns
    ]
    pr_keep = [
        c
        for c in ["试验阶段", "运营类型", "抽佣率", "试验分组", "试验起始日期", "试验结束日期"]
        if c in period_rate_df.columns
    ]

    tg = trial_group_df[tg_keep].copy() if tg_keep else pd.DataFrame()
    pr = period_rate_df[pr_keep].copy() if pr_keep else pd.DataFrame()

    for df in [tg, pr]:
        if "试验起始日期" in df.columns:
            df["试验起始日期"] = _to_date_series(df["试验起始日期"])
        if "试验结束日期" in df.columns:
            df["试验结束日期"] = _to_date_series(df["试验结束日期"])

    join_keys = ["试验分组", "试验起始日期", "试验结束日期"]
    if tg.empty:
        merged = pr.copy()
        merged["市名称"] = pd.NA
        merged["区域名称"] = pd.NA
        merged["区域类型"] = pd.NA
    elif pr.empty:
        merged = tg.copy()
        merged["试验阶段"] = pd.NA
        merged["运营类型"] = pd.NA
        merged["抽佣率"] = pd.NA
    else:
        merged = tg.merge(pr, on=join_keys, how="outer")

    if "市名称" in merged.columns or "区域名称" in merged.columns:
        city_series = (
            merged["市名称"] if "市名称" in merged.columns else pd.Series(pd.NA, index=merged.index)
        )
        area_series = (
            merged["区域名称"]
            if "区域名称" in merged.columns
            else pd.Series(pd.NA, index=merged.index)
        )
        merged["city_unit"] = city_series.fillna(area_series)
    else:
        merged["city_unit"] = pd.NA

    for col in cols:
        if col not in merged.columns:
            merged[col] = pd.NA

    merged = merged[cols].drop_duplicates().reset_index(drop=True)
    return merged


def compute_trial_phase_config_pivot(phase_wide_df: pd.DataFrame) -> pd.DataFrame:
    """将阶段配置宽表透视为运营类型列，用于城市分组配置展示。"""
    if phase_wide_df is None or phase_wide_df.empty:
        return pd.DataFrame()

    required = ["市名称", "试验分组", "试验阶段", "试验起始日期", "试验结束日期", "运营类型", "抽佣率"]
    missing = [c for c in required if c not in phase_wide_df.columns]
    if missing:
        return pd.DataFrame()

    base = phase_wide_df[required].copy()
    base["试验起始日期"] = _to_date_series(base["试验起始日期"])
    base["试验结束日期"] = _to_date_series(base["试验结束日期"])

    pivot = base.pivot_table(
        index=["市名称", "试验分组", "试验阶段", "试验起始日期", "试验结束日期"],
        columns="运营类型",
        values="抽佣率",
        aggfunc="first",
    ).reset_index()

    pivot.columns.name = None
    return pivot


def compute_trial_sku_profile(
    lark_data: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """构建试验 SKU 主数据。

    规则:
      - conf_trial_product_info INNER JOIN conf_product_info
      - JOIN 键: 日期 + 商品id
      - 输出参与过试验的商品及其最近参与日期
    """
    trial_df = lark_data.get("conf_trial_product_info", pd.DataFrame()).copy()
    product_df = lark_data.get("conf_product_info", pd.DataFrame()).copy()

    out_cols = ["商品id", "商品名称", "商家名称", "非试验区域抽佣率", "last_trial_date"]
    if trial_df.empty or product_df.empty:
        return pd.DataFrame(columns=out_cols)

    for df in [trial_df, product_df]:
        if "日期" in df.columns:
            df["日期"] = _to_date_series(df["日期"])
        if "商品id" in df.columns:
            df["商品id"] = pd.to_numeric(df["商品id"], errors="coerce")

    trial_keep = [c for c in ["日期", "商品id", "非试验区域抽佣率"] if c in trial_df.columns]
    product_keep = [
        c for c in ["日期", "商品id", "商品名称", "商家名称", "非试验区域抽佣率"] if c in product_df.columns
    ]
    if "日期" not in trial_keep or "商品id" not in trial_keep:
        return pd.DataFrame(columns=out_cols)
    if "日期" not in product_keep or "商品id" not in product_keep:
        return pd.DataFrame(columns=out_cols)

    trial_base = trial_df[trial_keep].copy().rename(
        columns={"非试验区域抽佣率": "非试验区域抽佣率_trial"}
    )
    product_base = product_df[product_keep].copy().rename(
        columns={"非试验区域抽佣率": "非试验区域抽佣率_prod"}
    )

    merged = trial_base.merge(product_base, on=["日期", "商品id"], how="inner")
    if merged.empty:
        return pd.DataFrame(columns=out_cols)

    trial_last = (
        trial_base.groupby("商品id", as_index=False)["日期"].max().rename(columns={"日期": "last_trial_date"})
    )
    merged = merged.merge(trial_last, on="商品id", how="left")

    merged = merged.sort_values(["商品id", "日期"], ascending=[True, False])
    latest = merged.drop_duplicates(subset=["商品id"], keep="first").copy()

    trial_rate = (
        latest["非试验区域抽佣率_trial"]
        if "非试验区域抽佣率_trial" in latest.columns
        else pd.Series(pd.NA, index=latest.index)
    )
    prod_rate = (
        latest["非试验区域抽佣率_prod"]
        if "非试验区域抽佣率_prod" in latest.columns
        else pd.Series(pd.NA, index=latest.index)
    )
    latest["非试验区域抽佣率"] = trial_rate.combine_first(prod_rate)

    latest["商品id"] = pd.to_numeric(latest["商品id"], errors="coerce").astype("Int64")
    for col in out_cols:
        if col not in latest.columns:
            latest[col] = pd.NA

    return latest[out_cols].reset_index(drop=True)


def compute_wide_table(
    lark_data: dict[str, pd.DataFrame],
    mc_data: dict[str, pd.DataFrame],
    target_date: date,
) -> pd.DataFrame:
    """计算核心聚合宽表。

    Steps:
        1. 预处理日期
        2. 公共过滤（有效订单 + 目标 SKU）
        3. 关联 conf_trial_group → city_unit + trial_group
        4. 关联 conf_trial_period_rate → stage
        5. 关联 conf_commission_adjustment → 参与试验类型过滤
        6. 关联 conf_product_info → sku_origin, sku_grade, sku_weight_spec
        7. 计算 stage_week, is_complete_week, trading_days
        8. 按阶段粒度聚合
    """
    lark_data = preprocess_lark_dates(lark_data)

    fact = mc_data.get("fact_order_item")
    if fact is None or fact.empty:
        logger.warning("fact_order_item is empty, returning empty wide table")
        return pd.DataFrame()

    # 重命名事实表列
    rename_map = {k: v for k, v in FIELD_MAPPING.items() if k in fact.columns}
    fact = fact.rename(columns=rename_map)

    # 确保日期列为 date 类型
    if "日期" in fact.columns:
        fact["日期"] = pd.to_datetime(fact["日期"], errors="coerce").dt.date

    # 公共过滤: 有效订单 + 目标 SKU
    if "is_valid" in fact.columns:
        fact = fact[fact["is_valid"] == 1]
    if "sku_id" in fact.columns:
        fact = fact[fact["sku_id"].isin(TARGET_SKU_IDS)]

    if fact.empty:
        logger.warning("No rows after public filters")
        return pd.DataFrame()

    # 关联 conf_county_info → 运营类型 (region_type)
    # 注意: MC SQL 已提供 市名称/省名称 等地域字段，无需从 county 取
    county_df = lark_data.get("conf_county_info", pd.DataFrame())
    if not county_df.empty and "county_id" in fact.columns:
        fact["county_id"] = fact["county_id"].astype(int)

        county_map = county_df[["区县id", "运营类型"]].copy()
        county_map["区县id"] = county_map["区县id"].astype(int)
        county_map = county_map.drop_duplicates(subset=["区县id"])

        fact = fact.merge(
            county_map, left_on="county_id", right_on="区县id", how="left"
        )
        # region_type 来自 conf_county_info.运营类型 (自营区域/代理人区域)
        fact["region_type"] = fact["运营类型"]
    else:
        fact["region_type"] = np.nan

    # 关联 conf_trial_group → city_unit, trial_group
    # MC SQL 已提供 市名称，直接用它关联试验分组
    trial_group_df = lark_data.get("conf_trial_group", pd.DataFrame())
    if not trial_group_df.empty and "市名称" in fact.columns:
        tg = trial_group_df.copy()
        tg_city = tg[["市名称", "区域名称", "试验分组"]].copy()
        tg_city = tg_city.rename(columns={"市名称": "city_unit_tg"})
        # 试验分组中 市名称 和 区域名称 可能不同（如 区域名称="萍乡市2", 市名称="萍乡市"）
        tg_city["join_key"] = tg_city["city_unit_tg"].fillna(tg_city["区域名称"])
        tg_city = tg_city[["join_key", "city_unit_tg", "试验分组"]].drop_duplicates(
            subset=["join_key"]
        )

        # 统一城市名用于关联，避免城市名包含空白导致无法匹配
        fact["city_join_key"] = _normalize_city_name(fact["市名称"])
        tg_city["join_key"] = _normalize_city_name(tg_city["join_key"])

        fact = fact.merge(
            tg_city,
            left_on="city_join_key",
            right_on="join_key",
            how="left",
        )
        # city_unit 使用试验分组中的市名称（归并后的名称）
        fact["city_unit"] = fact["city_unit_tg"].fillna(fact["市名称"])
        # 重命名中文列名为英文，便于后续聚合引用
        if "试验分组" in fact.columns:
            fact = fact.rename(columns={"试验分组": "trial_group"})

        # 仅保留试验分组命中的城市，避免非试验城市混入宽表
        if "trial_group" in fact.columns:
            fact = fact[fact["trial_group"].notna()]
    else:
        fact["city_unit"] = np.nan
        if "trial_group" not in fact.columns:
            fact["trial_group"] = np.nan

    # 关联 conf_trial_period_rate → stage
    period_rate_df = lark_data.get("conf_trial_period_rate", pd.DataFrame())
    if not period_rate_df.empty:
        stage = _derive_stage(fact["日期"], period_rate_df)
        fact["stage"] = stage
    else:
        fact["stage"] = np.nan

    # 关联 conf_commission_adjustment → 参与试验类型过滤
    adj_df = lark_data.get("conf_commission_adjustment", pd.DataFrame())
    if not adj_df.empty and "county_name" in fact.columns:
        adj = adj_df[["商品id", "区县名称", "参与试验类型"]].copy()
        adj = adj.rename(columns={"商品id": "sku_id", "区县名称": "county_name"})
        adj = adj.drop_duplicates(subset=["sku_id", "county_name"])

        fact = fact.merge(adj, on=["sku_id", "county_name"], how="left")

        # 过滤: 参与试验类型 contains "试验区域" AND not "非试验区域"
        if "参与试验类型" in fact.columns:
            mask = fact["参与试验类型"].apply(_is_trial_region)
            fact = fact[mask]
            # 转为字符串以支持后续 groupby
            fact["参与试验类型"] = fact["参与试验类型"].apply(
                lambda x: str(x) if not pd.isna(x) else x
            )

    # 关联 conf_product_info → sku_origin, sku_grade, sku_weight_spec
    product_df = lark_data.get("conf_product_info", pd.DataFrame())
    if not product_df.empty:
        prod = product_df[["商品id", "产地", "色号", "包装类型"]].copy()
        prod = prod.rename(
            columns={
                "商品id": "sku_id",
                "产地": "sku_origin",
                "色号": "sku_grade",
                "包装类型": "sku_weight_spec",
            }
        )
        prod = prod.drop_duplicates(subset=["sku_id"])
        fact = fact.merge(prod, on="sku_id", how="left")

    # region_type 已在上方从 conf_county_info.运营类型 获取

    # P1-4: 计算 stockout_num (per-row, 聚合时 sum)
    if "ordered_num" in fact.columns and "delivered_num" in fact.columns:
        fact["stockout_num"] = (fact["ordered_num"] - fact["delivered_num"]).clip(
            lower=0
        )

    # 计算 stage_week, is_complete_week
    dragon_boat = set(TRIAL_PHASE_CONFIG.get("dragon_boat_dates", []))
    fact = _compute_stage_week(fact, period_rate_df, dragon_boat)

    # 按阶段粒度聚合
    wide = _aggregate_by_stage(fact, dragon_boat, period_rate_df)

    logger.info(f"Wide table computed: {len(wide)} rows")
    return wide


def _derive_stage(dates: pd.Series, period_rate_df: pd.DataFrame) -> pd.Series:
    """根据日期和试验周期配置推导 stage。"""
    stages = pd.Series(index=dates.index, dtype="object")

    for _, row in period_rate_df.iterrows():
        start = row.get("试验起始日期")
        end = row.get("试验结束日期")
        phase = row.get("试验阶段")
        if pd.isna(start) or pd.isna(end) or pd.isna(phase):
            continue

        if isinstance(start, pd.Timestamp):
            start = start.date()
        if isinstance(end, pd.Timestamp):
            end = end.date()

        mask = (dates >= start) & (dates <= end)
        stages[mask & stages.isna()] = phase

    return stages


def _is_trial_region(value) -> bool:
    """判断参与试验类型是否为试验区域。

    公式字段返回: [{'text': '试验区域', 'type': 'text'}]
    或字符串: [试验区域] / [非试验区域]
    """
    if pd.isna(value):
        return False
    if isinstance(value, list):
        text = " ".join(
            item.get("text", str(item)) if isinstance(item, dict) else str(item)
            for item in value
        )
    else:
        text = str(value)
    return "试验区域" in text and "非" not in text


def _compute_stage_week(
    df: pd.DataFrame,
    period_rate_df: pd.DataFrame,
    dragon_boat: set,
) -> pd.DataFrame:
    """为生效期计算 stage_week 和 is_complete_week。"""
    df = df.copy()
    df["stage_week"] = np.nan
    df["is_complete_week"] = np.nan
    df["trading_days"] = np.nan

    if period_rate_df.empty:
        return df

    # 找到生效期的起止日期
    effect_rows = period_rate_df[period_rate_df["试验阶段"] == "生效期"]
    if effect_rows.empty:
        return df

    effect_start = effect_rows["试验起始日期"].min()
    if pd.isna(effect_start):
        return df
    if isinstance(effect_start, pd.Timestamp):
        effect_start = effect_start.date()

    effect_mask = df["stage"] == "生效期"
    if not effect_mask.any():
        return df

    effect_dates = df.loc[effect_mask, "日期"]
    for idx, d in effect_dates.items():
        if pd.isna(d):
            continue
        days_since_start = (d - effect_start).days
        week_num = days_since_start // 7 + 1
        df.at[idx, "stage_week"] = f"生效期_W{week_num}"

    # 计算每个 stage_week 的实际天数和是否完整
    if effect_mask.any():
        week_groups = df.loc[effect_mask].groupby("stage_week")["日期"].apply(set)
        for week_name, date_set in week_groups.items():
            is_complete = len(date_set) >= 7
            week_mask = effect_mask & (df["stage_week"] == week_name)
            df.loc[week_mask, "is_complete_week"] = is_complete
            df.loc[week_mask, "trading_days"] = len(date_set)

    # 摸底期 trading_days
    baseline_mask = df["stage"] == "摸底期"
    if baseline_mask.any():
        baseline_dates = df.loc[baseline_mask, "日期"].dropna()
        trading = set(baseline_dates) - dragon_boat
        df.loc[baseline_mask, "trading_days"] = len(trading) if trading else 0

    # 归一化预备期 trading_days = 1 (daily granularity)
    prep_mask = df["stage"] == "归一化预备期"
    df.loc[prep_mask, "trading_days"] = 1

    return df


def _aggregate_by_stage(
    df: pd.DataFrame, dragon_boat: set, period_rate_df: pd.DataFrame = None
) -> pd.DataFrame:
    """按阶段粒度聚合。"""
    agg_cols = {}
    if "order_item_id" in df.columns:
        agg_cols["order_item_id"] = "nunique"
    if "store_id" in df.columns:
        agg_cols["store_id"] = "nunique"
    if "gmv" in df.columns:
        agg_cols["gmv"] = "sum"
    if "commission_amount" in df.columns:
        agg_cols["commission_amount"] = "sum"
    if "supply_price_per_jin" in df.columns:
        agg_cols["supply_price_per_jin"] = "mean"
    if "stockout_num" in df.columns:
        agg_cols["stockout_num"] = "sum"

    results = []

    # 归一化预备期: stage × 日期 × city_unit × region_type
    prep = df[df["stage"] == "归一化预备期"]
    if not prep.empty:
        group_cols = ["stage", "日期", "city_unit", "region_type"]
        group_cols = [c for c in group_cols if c in prep.columns]
        g = prep.groupby(group_cols, dropna=False).agg(agg_cols)
        results.append(g.reset_index())

    # 摸底期: stage × city_unit × sku_id (+ region_type, trial_group)
    baseline = df[df["stage"] == "摸底期"]
    if not baseline.empty:
        group_cols = ["stage", "city_unit", "sku_id", "region_type", "trial_group"]
        group_cols = [c for c in group_cols if c in baseline.columns]
        g = baseline.groupby(group_cols, dropna=False).agg(agg_cols)
        results.append(g.reset_index())

    # 生效期: stage_week × city_unit × sku_id (+ region_type, trial_group)
    effect = df[df["stage"] == "生效期"]
    if not effect.empty:
        group_cols = [
            "stage",
            "stage_week",
            "city_unit",
            "sku_id",
            "region_type",
            "trial_group",
        ]
        group_cols = [c for c in group_cols if c in effect.columns]
        g = effect.groupby(group_cols, dropna=False).agg(agg_cols)
        results.append(g.reset_index())

    if not results:
        return pd.DataFrame()

    wide = pd.concat(results, ignore_index=True)

    # 重命名聚合列
    rename = {
        "order_item_id": "order_count",
        "store_id": "active_store_count",
        "supply_price_per_jin": "supply_price",
    }
    wide = wide.rename(columns={k: v for k, v in rename.items() if k in wide.columns})

    # 计算 commission_rate
    if "commission_amount" in wide.columns and "gmv" in wide.columns:
        wide["commission_rate"] = np.where(
            wide["gmv"] > 0, wide["commission_amount"] / wide["gmv"], np.nan
        )

    # P1-5: 关联 target_r0 from conf_trial_period_rate
    if period_rate_df is not None and not period_rate_df.empty:
        target_map = period_rate_df.copy()
        if "trial_group" in wide.columns and "stage" in wide.columns:
            # 构建 (stage, trial_group) → target_r0 映射
            # 运营类型需要从 region_type 映射: 自营区域/代理人区域
            r0_rows = []
            for _, row in target_map.iterrows():
                phase = row.get("试验阶段")
                op_type = row.get("运营类型", "")
                group = row.get("试验分组")
                r0 = row.get("抽佣率")
                if pd.notna(phase) and pd.notna(r0):
                    r0_rows.append(
                        {
                            "stage": phase,
                            "trial_group": group,
                            "region_type": op_type,
                            "target_r0": r0,
                        }
                    )
            if r0_rows:
                r0_df = pd.DataFrame(r0_rows).drop_duplicates(
                    subset=["stage", "trial_group", "region_type"]
                )
                join_cols = [
                    c
                    for c in ["stage", "trial_group", "region_type"]
                    if c in wide.columns
                ]
                if join_cols:
                    for col in join_cols:
                        if wide[col].dtype != object:
                            wide[col] = wide[col].astype(str).replace("nan", pd.NA)
                    wide = wide.merge(r0_df, on=join_cols, how="left")

    return wide


# ==========================================================================
# 功效分析
# ==========================================================================


def _compute_sigma(fact_df: pd.DataFrame, sku_id: int) -> dict:
    """计算单个 SKU 的 σ (GMV 变异系数均值)。

    Steps:
        1. 筛选该 SKU 数据
        2. 按城市计算 4 周 GMV 的 CV = σ / μ
        3. σ_raw = 8 城市 CV 均值
        4. σ_adjusted = σ_raw × 1.5
    """
    sku_data = fact_df[fact_df["sku_id"] == sku_id].copy()
    if sku_data.empty:
        return {"sku_id": sku_id, "sigma_raw": np.nan, "sigma_adjusted": np.nan}

    cvs = []
    for city, group in sku_data.groupby("city_unit"):
        gmv_values = group["gmv"].values
        if len(gmv_values) < 2:
            continue
        mean_gmv = np.mean(gmv_values)
        if mean_gmv == 0:
            continue
        std_gmv = np.std(gmv_values, ddof=1)
        cv = std_gmv / mean_gmv
        cvs.append(cv)

    if not cvs:
        return {"sku_id": sku_id, "sigma_raw": np.nan, "sigma_adjusted": np.nan}

    sigma_raw = np.mean(cvs)
    return {
        "sku_id": sku_id,
        "sigma_raw": sigma_raw,
        "sigma_adjusted": sigma_raw * 1.5,
    }


def _compute_rho(
    fact_df: pd.DataFrame,
    sku_id: int,
    week_pairs: list[tuple[str, str]],
) -> dict:
    """计算单个 SKU 的 ρ (城市间 GMV 相关系数)。

    Args:
        week_pairs: 周对列表 [(w_a, w_b), ...]

    Returns:
        dict with rho_values (list) and rho_main (min)
    """
    sku_data = fact_df[fact_df["sku_id"] == sku_id].copy()
    if sku_data.empty:
        return {"sku_id": sku_id, "rho_values": [], "rho_main": np.nan}

    rho_values = []
    for w_a, w_b in week_pairs:
        gmv_a = sku_data[sku_data["week_id"] == w_a].set_index("city_unit")["gmv"]
        gmv_b = sku_data[sku_data["week_id"] == w_b].set_index("city_unit")["gmv"]

        common_cities = gmv_a.index.intersection(gmv_b.index)
        if len(common_cities) < 3:
            continue

        vec_a = gmv_a.loc[common_cities].values
        vec_b = gmv_b.loc[common_cities].values

        if np.std(vec_a) == 0 or np.std(vec_b) == 0:
            continue

        corr = np.corrcoef(vec_a, vec_b)[0, 1]
        rho_values.append(corr)

    rho_main = min(rho_values) if rho_values else np.nan
    return {"sku_id": sku_id, "rho_values": rho_values, "rho_main": rho_main}


def _compute_power(sigma_adjusted: float, rho_main: float, n_actual: int = 2) -> dict:
    """功效验证公式: n_required = 4 × σ² × (1 - ρ) × 7.84 / δ²

    Parameters:
        z_α/2 = 1.96, z_β = 0.84, (z_α/2 + z_β)² ≈ 7.84
        δ = 0.10 (MDE = 10%)
    """
    if np.isnan(sigma_adjusted) or np.isnan(rho_main):
        return {
            "sigma_adjusted": sigma_adjusted,
            "rho_main": rho_main,
            "n_required": np.nan,
            "n_actual": n_actual,
            "power_sufficient": False,
        }

    delta = 0.10
    n_required = 4 * sigma_adjusted**2 * (1 - rho_main) * 7.84 / delta**2
    return {
        "sigma_adjusted": sigma_adjusted,
        "rho_main": rho_main,
        "n_required": n_required,
        "n_actual": n_actual,
        "power_sufficient": n_required <= n_actual,
    }


def _compute_cross_correlation(sku_dfs: dict[int, pd.DataFrame]) -> list[dict]:
    """计算 SKU 间 GMV 相关系数（3 对）。"""
    sku_ids = sorted(sku_dfs.keys())
    results = []

    for i in range(len(sku_ids)):
        for j in range(i + 1, len(sku_ids)):
            sku_a, sku_b = sku_ids[i], sku_ids[j]
            df_a = sku_dfs[sku_a].set_index("city_unit")["gmv"]
            df_b = sku_dfs[sku_b].set_index("city_unit")["gmv"]

            common = df_a.index.intersection(df_b.index)
            if len(common) < 3:
                results.append({"sku_a": sku_a, "sku_b": sku_b, "rho": np.nan})
                continue

            corr = np.corrcoef(df_a.loc[common].values, df_b.loc[common].values)[0, 1]
            results.append({"sku_a": sku_a, "sku_b": sku_b, "rho": corr})

    return results


def compute_power_analysis(
    fact_df: pd.DataFrame,
    config: dict,
) -> pd.DataFrame:
    """功效分析主入口。

    Args:
        fact_df: 交易事实数据 (需含 sku_id, city_unit, gmv, 日期)
        config:  TRIAL_PHASE_CONFIG

    Returns:
        功效分析结果 DataFrame (每 SKU 一行)
    """
    from .config import TARGET_SKU_IDS

    # 按历史基线范围筛选
    baseline_ranges = config.get("historical_baseline_ranges", [])
    filtered = pd.DataFrame()
    for start, end in baseline_ranges:
        mask = (fact_df["日期"] >= start) & (fact_df["日期"] <= end)
        filtered = pd.concat([filtered, fact_df[mask]])

    if filtered.empty:
        logger.warning("No historical baseline data for power analysis")
        return pd.DataFrame()

    # 按 SKU × 城市 × 周聚合 GMV
    filtered = filtered.copy()
    filtered["日期"] = pd.to_datetime(filtered["日期"], errors="coerce").dt.date

    # 构造 week_id: 按自然周
    filtered["week_id"] = filtered["日期"].apply(
        lambda d: f"W{d.isocalendar()[1]}" if d else None
    )

    weekly = (
        filtered.groupby(["sku_id", "city_unit", "week_id"])["gmv"].sum().reset_index()
    )

    # 定义周对 (pre: W1/W2, post: W3/W4)
    all_weeks = sorted(weekly["week_id"].unique())
    week_pairs = []
    if len(all_weeks) >= 4:
        mid = len(all_weeks) // 2
        week_pairs.append((all_weeks[0], all_weeks[1]))
        if mid + 1 < len(all_weeks):
            week_pairs.append((all_weeks[mid], all_weeks[mid + 1]))

    results = []
    sku_gmv_dfs = {}

    for sku_id in TARGET_SKU_IDS:
        sku_weekly = weekly[weekly["sku_id"] == sku_id]

        # σ
        sigma_result = _compute_sigma(sku_weekly, sku_id)

        # ρ
        rho_result = _compute_rho(sku_weekly, sku_id, week_pairs)

        # 功效
        power_result = _compute_power(
            sigma_result["sigma_adjusted"],
            rho_result["rho_main"],
        )

        # 检查数据充分性
        n_weeks = sku_weekly["week_id"].nunique()
        fallback = n_weeks < 3

        row = {
            "sku_id": sku_id,
            **sigma_result,
            "rho_pre": (
                rho_result["rho_values"][0]
                if len(rho_result["rho_values"]) > 0
                else np.nan
            ),
            "rho_post": (
                rho_result["rho_values"][1]
                if len(rho_result["rho_values"]) > 1
                else np.nan
            ),
            "rho_main": rho_result["rho_main"],
            **power_result,
            "fallback": fallback,
            "n_weeks_available": n_weeks,
        }
        results.append(row)

        # 保存用于交叉相关
        sku_city_gmv = sku_weekly.groupby("city_unit")["gmv"].sum().reset_index()
        sku_gmv_dfs[sku_id] = sku_city_gmv

    # 交叉相关
    cross_corr = _compute_cross_correlation(sku_gmv_dfs)

    result_df = pd.DataFrame(results)
    # 附加交叉相关信息
    if cross_corr:
        result_df.attrs["cross_correlation"] = cross_corr

    return result_df
