# coding:utf8
"""Tab 2: 配置核查 (H-1 抽佣率 + H-2 供货价)"""

import json

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from workers.cr_analyze.config import TARGET_R0_REFERENCE


def _extract_participation_text(value) -> str:
    """将多维表格公式字段值标准化为可读文本（试验区域/非试验区域）。"""
    if isinstance(value, list):
        texts = []
        for item in value:
            if isinstance(item, dict):
                texts.append(str(item.get("text", "")).strip())
            else:
                texts.append(str(item).strip())
        text = " ".join([t for t in texts if t])
    elif isinstance(value, dict):
        text = str(value.get("text", "")).strip()
    elif value is None:
        return ""
    else:
        if pd.isna(value):
            return ""
        raw = str(value).strip()
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
                return _extract_participation_text(parsed)
            except Exception:
                text = raw.strip("[]")
            else:
                text = raw
        else:
            text = raw

    if "非试验区域" in text:
        return "非试验区域"
    if "试验区域" in text:
        return "试验区域"
    return text


def _extract_city_from_region_full_name(region_full_name) -> str:
    """从区域全称提取城市名，形如 省-市-区县。"""
    if region_full_name is None:
        return ""
    text = str(region_full_name).strip()
    if not text:
        return ""
    parts = [p.strip() for p in text.split("-") if p and str(p).strip()]
    if len(parts) >= 2:
        city = parts[1]
    else:
        city = parts[0] if parts else ""
    # 去掉尾部数字（如 萍乡市2）以对齐区县侧城市名
    city = str(city).strip()
    while city and city[-1].isdigit():
        city = city[:-1]
    return city.strip()


def _aggregate_trial_city_daily(
    df: pd.DataFrame,
    value_col: str,
) -> pd.DataFrame:
    """按 日期+商品id+城市 聚合为单行，避免多城市图重复点。"""
    work = df.copy()
    keys = ["日期", "商品id", "城市名称"]
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    work = work.dropna(subset=keys + [value_col])
    if work.empty:
        return work
    agg = (
        work.groupby(keys, as_index=False)[value_col]
        .mean()
        .sort_values(["商品id", "城市名称", "日期"])
    )
    return agg


def render(data: dict[str, pd.DataFrame]):
    adj = data.get("conf_commission_adjustment", pd.DataFrame())
    period_rate = data.get("conf_trial_period_rate", pd.DataFrame())
    product_info = data.get("conf_product_info", pd.DataFrame())
    trial_region_price = data.get("conf_trial_region_price", pd.DataFrame())
    trial_sku_profile = data.get("trial_sku_profile", pd.DataFrame())

    # H-1: 抽佣率配置核查
    st.subheader("H-1: 抽佣率配置核查")
    if not adj.empty and not period_rate.empty:
        check_df = adj.copy()

        # 基础清洗
        if "日期" in check_df.columns:
            check_df["日期"] = pd.to_datetime(check_df["日期"], errors="coerce").dt.date
        if "商品id" in check_df.columns:
            check_df["商品id"] = pd.to_numeric(check_df["商品id"], errors="coerce")

        # 标记参与试验类型（只保留可读文本）
        if "参与试验类型" in check_df.columns:
            check_df["参与试验类型"] = check_df["参与试验类型"].apply(_extract_participation_text)

        # 补齐商品名称 + 非试验区域抽佣率
        sku_info = pd.DataFrame()
        if trial_sku_profile is not None and not trial_sku_profile.empty:
            keep = [c for c in ["商品id", "商品名称", "非试验区域抽佣率"] if c in trial_sku_profile.columns]
            if keep:
                sku_info = trial_sku_profile[keep].copy()

        if sku_info.empty and not product_info.empty:
            prod = product_info.copy()
            if "日期" in prod.columns:
                prod["日期"] = pd.to_datetime(prod["日期"], errors="coerce").dt.date
            if "商品id" in prod.columns:
                prod["商品id"] = pd.to_numeric(prod["商品id"], errors="coerce")
            keep = [
                c
                for c in ["日期", "商品id", "商品名称", "非试验区域抽佣率"]
                if c in prod.columns
            ]
            if keep:
                base = prod[keep].copy().sort_values("日期", ascending=False)
                sku_info = base.drop_duplicates(subset=["商品id"], keep="first")
                sku_info = sku_info[[c for c in ["商品id", "商品名称", "非试验区域抽佣率"] if c in sku_info.columns]]

        if not sku_info.empty and "商品id" in check_df.columns:
            check_df = check_df.merge(sku_info, on="商品id", how="left")

        # 补齐 H-1 价格列：试验区域与非试验区域使用不同来源
        # - 试验区域: conf_trial_region_price (按 日期+商品id+城市名称)
        # - 非试验区域: conf_product_info (按 日期+商品id)
        if "日期" in check_df.columns and "商品id" in check_df.columns:
            # 非试验区域价格映射
            non_trial_price = pd.DataFrame()
            if not product_info.empty:
                npdf = product_info.copy()
                if "日期" in npdf.columns:
                    npdf["日期"] = pd.to_datetime(npdf["日期"], errors="coerce").dt.date
                if "商品id" in npdf.columns:
                    npdf["商品id"] = pd.to_numeric(npdf["商品id"], errors="coerce")
                keep = [
                    c
                    for c in [
                        "日期",
                        "商品id",
                        "非试验区域商家供货斤单价",
                        "非试验区域平台销售斤单价",
                    ]
                    if c in npdf.columns
                ]
                if {"日期", "商品id"}.issubset(keep):
                    non_trial_price = npdf[keep].copy().drop_duplicates(subset=["日期", "商品id"], keep="first")
                    non_trial_price = non_trial_price.rename(
                        columns={
                            "非试验区域商家供货斤单价": "_non_trial_supply_price",
                            "非试验区域平台销售斤单价": "_non_trial_platform_price",
                        }
                    )

            # 试验区域价格映射（城市粒度）
            trial_price = pd.DataFrame()
            if not trial_region_price.empty:
                tpdf = trial_region_price.copy()
                if "日期" in tpdf.columns:
                    tpdf["日期"] = pd.to_datetime(tpdf["日期"], errors="coerce").dt.date
                if "商品id" in tpdf.columns:
                    tpdf["商品id"] = pd.to_numeric(tpdf["商品id"], errors="coerce")
                if "区域全称" in tpdf.columns:
                    tpdf["城市名称"] = tpdf["区域全称"].apply(_extract_city_from_region_full_name)
                keep = [
                    c
                    for c in [
                        "日期",
                        "商品id",
                        "城市名称",
                        "试验区域商家供货斤单价",
                        "试验区域平台销售斤单价",
                    ]
                    if c in tpdf.columns
                ]
                if {"日期", "商品id", "城市名称"}.issubset(keep):
                    trial_price = tpdf[keep].copy()
                    for vc in ["试验区域商家供货斤单价", "试验区域平台销售斤单价"]:
                        if vc in trial_price.columns:
                            trial_price[vc] = pd.to_numeric(trial_price[vc], errors="coerce")
                    agg_map = {}
                    if "试验区域商家供货斤单价" in trial_price.columns:
                        agg_map["试验区域商家供货斤单价"] = "mean"
                    if "试验区域平台销售斤单价" in trial_price.columns:
                        agg_map["试验区域平台销售斤单价"] = "mean"
                    trial_price = (
                        trial_price.groupby(["日期", "商品id", "城市名称"], as_index=False)
                        .agg(agg_map)
                        .rename(
                            columns={
                                "试验区域商家供货斤单价": "_trial_supply_price",
                                "试验区域平台销售斤单价": "_trial_platform_price",
                            }
                        )
                    )

            if "区域全称" in check_df.columns:
                check_df["区域全称"] = check_df["区域全称"].astype(str).str.strip()
                check_df["城市名称"] = check_df["区域全称"].apply(_extract_city_from_region_full_name)

            if not non_trial_price.empty:
                check_df = check_df.merge(non_trial_price, on=["日期", "商品id"], how="left")
            if not trial_price.empty and "城市名称" in check_df.columns:
                check_df = check_df.merge(trial_price, on=["日期", "商品id", "城市名称"], how="left")

            is_trial_mask = check_df.get("参与试验类型") == "试验区域"
            check_df["商家供货斤单价"] = np.where(
                is_trial_mask,
                pd.to_numeric(check_df.get("_trial_supply_price"), errors="coerce"),
                pd.to_numeric(check_df.get("_non_trial_supply_price"), errors="coerce"),
            )
            check_df["商城销售斤单价"] = np.where(
                is_trial_mask,
                pd.to_numeric(check_df.get("_trial_platform_price"), errors="coerce"),
                pd.to_numeric(check_df.get("_non_trial_platform_price"), errors="coerce"),
            )

            check_df = check_df.drop(
                columns=[
                    "城市名称",
                    "_non_trial_supply_price",
                    "_non_trial_platform_price",
                    "_trial_supply_price",
                    "_trial_platform_price",
                ],
                errors="ignore",
            )

        # 构建筛选组件（多列布局）
        filter_box = st.container()
        with filter_box:
            c1, c2, c3 = st.columns(3)

            selected_date = None
            available_dates = []
            if "日期" in check_df.columns:
                available_dates = sorted([d for d in check_df["日期"].dropna().unique()])
            with c1:
                if available_dates:
                    selected_date = st.date_input(
                        "日期筛选",
                        value=available_dates[-1],
                        min_value=available_dates[0],
                        max_value=available_dates[-1],
                        key="tab2_h1_date",
                    )

            with c2:
                participation_opts = []
                if "参与试验类型" in check_df.columns:
                    participation_opts = [
                        x
                        for x in ["试验区域", "非试验区域"]
                        if x in set(check_df["参与试验类型"].dropna().tolist())
                    ]
                selected_participation = st.multiselect(
                    "参与试验类型",
                    options=participation_opts,
                    default=participation_opts,
                    key="tab2_h1_participation",
                )

            with c3:
                product_opts = []
                if "商品名称" in check_df.columns:
                    product_opts = sorted(check_df["商品名称"].dropna().astype(str).unique().tolist())
                selected_products = st.multiselect(
                    "商品名称",
                    options=product_opts,
                    default=product_opts,
                    key="tab2_h1_product_name",
                )

        # 应用筛选
        if selected_date is not None and "日期" in check_df.columns:
            check_df = check_df[check_df["日期"] == selected_date]
        if "参与试验类型" in check_df.columns and selected_participation:
            check_df = check_df[check_df["参与试验类型"].isin(selected_participation)]
        if "商品名称" in check_df.columns and selected_products:
            check_df = check_df[check_df["商品名称"].isin(selected_products)]

        # 衍生试验区域标记
        if "参与试验类型" in check_df.columns:
            check_df["is_trial"] = check_df["参与试验类型"] == "试验区域"
            non_trial = check_df[~check_df["is_trial"]]
            if not non_trial.empty:
                st.warning(f"发现 {len(non_trial)} 条非试验区域记录")

        # 构建目标 r₀ 映射表 (当前阶段 × 试验分组 → target_r)
        # 使用 conf_trial_group 获取试验分组
        trial_group = data.get("conf_trial_group", pd.DataFrame())
        if not trial_group.empty and "区县名称" in check_df.columns:
            # 通过区县名称 → conf_county_info → 市名称 → conf_trial_group → 试验分组
            county_info = data.get("conf_county_info", pd.DataFrame())
            if not county_info.empty:
                county_info = county_info.copy()
                if "日期" in county_info.columns:
                    county_info["日期"] = pd.to_datetime(
                        county_info["日期"], errors="coerce"
                    ).dt.date
                    # 区县映射固定使用最新快照日期
                    latest_day = county_info["日期"].max()
                    county_info = county_info[county_info["日期"] == latest_day]

                county_cols = [c for c in ["区县名称", "市名称", "运营类型"] if c in county_info.columns]
                county_to_city = county_info[county_cols].drop_duplicates()
                tg_city = trial_group[["市名称", "试验分组"]].drop_duplicates()
                county_to_group = county_to_city.merge(tg_city, on="市名称", how="left")

                # 每个区县仅保留一条映射，优先保留试验分组非空记录
                county_to_group["_group_notna"] = county_to_group["试验分组"].notna()
                county_to_group = county_to_group.sort_values(
                    ["区县名称", "_group_notna"],
                    ascending=[True, False],
                ).drop_duplicates(subset=["区县名称"], keep="first")
                county_to_group = county_to_group.drop(columns=["_group_notna"], errors="ignore")

                check_df = check_df.merge(
                    county_to_group[[c for c in ["区县名称", "试验分组", "运营类型"] if c in county_to_group.columns]],
                    on="区县名称",
                    how="left",
                )

                # 同一商品/日期/区县若出现多条，优先保留试验分组非空记录
                dedup_keys = [
                    c for c in ["日期", "商品id", "区县名称", "区域全称", "参与试验类型"] if c in check_df.columns
                ]
                if dedup_keys:
                    check_df["_group_notna"] = check_df["试验分组"].notna()
                    check_df = check_df.sort_values(
                        dedup_keys + ["_group_notna"],
                        ascending=[True] * len(dedup_keys) + [False],
                    ).drop_duplicates(subset=dedup_keys, keep="first")
                    check_df = check_df.drop(columns=["_group_notna"], errors="ignore")

        # 计算配置偏差: 使用固定抽佣率调整作为 configured_r
        if "固定抽佣率调整" in check_df.columns:
            # 获取当前阶段的目标 r₀
            from datetime import date as dt_date

            today = dt_date.today()
            current_stage = None
            if not period_rate.empty:
                pr = period_rate.copy()
                for col in ["试验起始日期", "试验结束日期"]:
                    if col in pr.columns:
                        pr[col] = pd.to_datetime(pr[col], errors="coerce").dt.date
                for _, row in pr.iterrows():
                    s, e = row.get("试验起始日期"), row.get("试验结束日期")
                    if pd.notna(s) and pd.notna(e) and s <= today <= e:
                        current_stage = row.get("试验阶段")
                        break

            # 查找 target_r: 按 (stage, trial_group) 从 TARGET_R0_REFERENCE 获取
            if current_stage and current_stage in TARGET_R0_REFERENCE:
                ref = TARGET_R0_REFERENCE[current_stage]
                
                def get_target_r(row):
                    group = row.get("试验分组")
                    region_type = row.get("运营类型")
                    if pd.isna(group) or pd.isna(region_type):
                        return np.nan
                    return ref.get(str(region_type), {}).get(str(group), np.nan)

                check_df["试验抽佣率需求"] = check_df.apply(get_target_r, axis=1)
                # 固定抽佣率调整（按系数百分比调整后）
                check_df["固定抽佣率调整"] = (
                    pd.to_numeric(check_df.get("固定抽佣率调整"), errors="coerce")
                    * pd.to_numeric(check_df.get("调整系数"), errors="coerce")
                    / 100.0
                )

                # 配置预警:
                # 非试验区域 -> 绿灯
                # 试验区域 -> 非试验区域抽佣率 + 固定抽佣率调整 == 试验抽佣率需求 ? 绿灯 : 红灯
                base_r = pd.to_numeric(check_df.get("非试验区域抽佣率"), errors="coerce")
                fixed_r = pd.to_numeric(check_df.get("固定抽佣率调整"), errors="coerce")
                target_r = pd.to_numeric(check_df.get("试验抽佣率需求"), errors="coerce")
                calc_r = base_r + fixed_r
                equal_mask = np.isclose(calc_r, target_r, atol=1e-6, equal_nan=False)

                check_df["配置预警"] = np.where(
                    check_df.get("参与试验类型") == "非试验区域",
                    "🟢",
                    np.where(equal_mask, "🟢", "🔴"),
                )

                check_df["r_deviation"] = (
                    check_df["固定抽佣率调整"] - check_df["试验抽佣率需求"]
                )

                # 保留内部偏差计算用于核查，不在表格中单独展示

        # 隐形物流费加价独立计算（不依赖试验阶段匹配）
        check_df["隐形物流费加价"] = (
            pd.to_numeric(check_df.get("调整系数"), errors="coerce")
            * pd.to_numeric(check_df.get("固定抽佣金额调整"), errors="coerce")
        )

        # 显示偏差表
        display_cols = [
            c
            for c in [
                "日期",
                "商品id",
                "商品名称",
                "区县名称",
                "区域全称",
                "参与试验类型",
                "试验分组",
                "商家供货斤单价",
                "商城销售斤单价",
                "非试验区域抽佣率",
                "固定抽佣率调整",
                "试验抽佣率需求",
                "配置预警",
                "隐形物流费加价",
            ]
            if c in check_df.columns
        ]

        # 排序: 参与试验类型（试验区域→非试验区域）→ 区域全称
        if "参与试验类型" in check_df.columns:
            check_df["参与试验类型"] = pd.Categorical(
                check_df["参与试验类型"],
                categories=["试验区域", "非试验区域"],
                ordered=True,
            )
        sort_cols = [c for c in ["参与试验类型", "区域全称"] if c in check_df.columns]
        if sort_cols:
            check_df = check_df.sort_values(sort_cols, na_position="last")
        if "参与试验类型" in check_df.columns:
            check_df["参与试验类型"] = check_df["参与试验类型"].astype(str)

        if display_cols:
            st.dataframe(check_df[display_cols].head(100), use_container_width=True)
        else:
            st.dataframe(check_df.head(100), use_container_width=True)
    elif not adj.empty:
        st.dataframe(adj.head(100), use_container_width=True)
    else:
        st.info("暂无抽佣率调整数据")

    # H-2: 商品价格和抽佣率趋势
    st.subheader("H-2: 商品价格和抽佣率趋势")
    if product_info.empty and trial_region_price.empty:
        st.info("暂无商品价格与抽佣率数据")
        return

    # 商品筛选器：仅展示“试验商品 且 当日可售卖”
    trial_sku_set = set()
    if trial_sku_profile is not None and not trial_sku_profile.empty and "商品id" in trial_sku_profile.columns:
        trial_ids = pd.to_numeric(trial_sku_profile["商品id"], errors="coerce")
        trial_sku_set = set(trial_ids.dropna().tolist())
    elif not trial_region_price.empty and "商品id" in trial_region_price.columns:
        trial_ids = pd.to_numeric(trial_region_price["商品id"], errors="coerce")
        trial_sku_set = set(trial_ids.dropna().tolist())

    sellable_today_set = set()
    sellable_ref_date = None
    if (
        not product_info.empty
        and {"日期", "商品id", "是否当日上架"}.issubset(product_info.columns)
    ):
        sell_df = product_info.copy()
        sell_df["日期"] = pd.to_datetime(sell_df["日期"], errors="coerce")
        sell_df["商品id"] = pd.to_numeric(sell_df["商品id"], errors="coerce")
        sell_df["是否当日上架"] = pd.to_numeric(sell_df["是否当日上架"], errors="coerce")
        sellable_ref_date = sell_df["日期"].max()
        if pd.notna(sellable_ref_date):
            sell_df = sell_df[sell_df["日期"] == sellable_ref_date]
            sellable_today_set = set(
                sell_df[sell_df["是否当日上架"] == 1]["商品id"].dropna().tolist()
            )

    eligible_sku_set = trial_sku_set.intersection(sellable_today_set)

    if not eligible_sku_set:
        st.info("暂无满足条件的商品：仅展示试验商品且当日可售卖（是否当日上架=1）")
        return

    if sellable_ref_date is not None and pd.notna(sellable_ref_date):
        st.caption(
            f"商品范围：试验商品且当日可售卖（基准日期：{pd.Timestamp(sellable_ref_date).strftime('%Y-%m-%d')}）"
        )

    sku_candidates = []
    if not product_info.empty and "商品id" in product_info.columns:
        pmeta = product_info.copy()
        if "日期" in pmeta.columns:
            pmeta["日期"] = pd.to_datetime(pmeta["日期"], errors="coerce")
        pmeta["商品id"] = pd.to_numeric(pmeta["商品id"], errors="coerce")
        if "商品名称" not in pmeta.columns:
            pmeta["商品名称"] = ""
        pmeta = pmeta[pmeta["商品id"].isin(eligible_sku_set)]
        keep_cols = [c for c in ["日期", "商品id", "商品名称"] if c in pmeta.columns]
        sku_candidates.append(pmeta[keep_cols])
    if not trial_region_price.empty and "商品id" in trial_region_price.columns:
        tmeta = trial_region_price.copy()
        if "日期" in tmeta.columns:
            tmeta["日期"] = pd.to_datetime(tmeta["日期"], errors="coerce")
        tmeta["商品id"] = pd.to_numeric(tmeta["商品id"], errors="coerce")
        if "商品名称" not in tmeta.columns:
            tmeta["商品名称"] = ""
        tmeta = tmeta[tmeta["商品id"].isin(eligible_sku_set)]
        keep_cols = [c for c in ["日期", "商品id", "商品名称"] if c in tmeta.columns]
        sku_candidates.append(tmeta[keep_cols])

    if not sku_candidates:
        st.info("缺少商品id字段，无法渲染 H-2")
        return

    sku_meta = pd.concat(sku_candidates, ignore_index=True)
    sku_meta = sku_meta.dropna(subset=["商品id"])
    sku_meta["商品名称"] = sku_meta["商品名称"].fillna("").astype(str)
    sku_meta["_name_not_empty"] = sku_meta["商品名称"].str.strip().ne("")
    if "日期" in sku_meta.columns:
        sku_meta = sku_meta.sort_values(["商品id", "日期", "_name_not_empty"], ascending=[True, False, False])
    else:
        sku_meta = sku_meta.sort_values(["商品id", "_name_not_empty"], ascending=[True, False])
    sku_meta = sku_meta.drop_duplicates(subset=["商品id"], keep="first")

    if sku_meta.empty:
        st.info("暂无可选择商品")
        return

    sku_options = sku_meta["商品id"].tolist()
    sku_name_map = dict(zip(sku_meta["商品id"], sku_meta["商品名称"]))

    def _sku_label(sku):
        name = sku_name_map.get(sku, "")
        try:
            sku_text = str(int(float(sku)))
        except Exception:
            sku_text = str(sku)
        return f"{sku_text} - {name}" if name else sku_text

    selected_sku = st.selectbox(
        "商品筛选",
        options=sku_options,
        format_func=_sku_label,
        key="tab2_h2_sku",
    )

    # 图1：非试验区域（双轴）- 商家供货斤单价 + 抽佣率
    st.markdown("**图1：非试验区域（双轴）- 商家供货斤单价与抽佣率**")
    non_trial_supply_col = "非试验区域商家供货斤单价"
    non_trial_rate_col = "非试验区域抽佣率"
    if (
        not product_info.empty
        and {"日期", "商品id", non_trial_supply_col, non_trial_rate_col}.issubset(product_info.columns)
    ):
        nt = product_info.copy()
        nt["日期"] = pd.to_datetime(nt["日期"], errors="coerce")
        nt["商品id"] = pd.to_numeric(nt["商品id"], errors="coerce")
        nt = nt[nt["商品id"] == selected_sku].copy()
        nt = nt.sort_values("日期")
        nt[non_trial_supply_col] = pd.to_numeric(nt[non_trial_supply_col], errors="coerce")
        nt[non_trial_rate_col] = pd.to_numeric(nt[non_trial_rate_col], errors="coerce")
        nt = nt.dropna(subset=["日期"], how="any")

        if nt[[non_trial_supply_col, non_trial_rate_col]].dropna(how="all").empty:
            st.info("该商品暂无非试验区域供货价/抽佣率可视化数据")
        else:
            supply_text = nt[non_trial_supply_col].map(
                lambda x: "" if pd.isna(x) else f"{x:.2f}"
            )
            rate_text = nt[non_trial_rate_col].map(
                lambda x: "" if pd.isna(x) else f"{x:.2%}"
            )
            fig_non_trial = make_subplots(specs=[[{"secondary_y": True}]])
            fig_non_trial.add_trace(
                go.Scatter(
                    x=nt["日期"],
                    y=nt[non_trial_supply_col],
                    mode="lines+markers+text",
                    text=supply_text,
                    textposition="top center",
                    textfont=dict(size=11),
                    name="非试验区域商家供货斤单价",
                    cliponaxis=False,
                ),
                secondary_y=False,
            )
            fig_non_trial.add_trace(
                go.Scatter(
                    x=nt["日期"],
                    y=nt[non_trial_rate_col],
                    mode="lines+markers+text",
                    text=rate_text,
                    textposition="top center",
                    textfont=dict(size=11),
                    name="非试验区域抽佣率",
                    cliponaxis=False,
                ),
                secondary_y=True,
            )
            fig_non_trial.update_layout(height=360, margin=dict(l=20, r=20, t=30, b=20))
            fig_non_trial.update_xaxes(title_text="日期", tickformat="%Y-%m-%d")
            fig_non_trial.update_yaxes(title_text="商家供货斤单价", rangemode="tozero", secondary_y=False)
            fig_non_trial.update_yaxes(title_text="抽佣率", tickformat=".2%", rangemode="tozero", secondary_y=True)
            st.plotly_chart(fig_non_trial, use_container_width=True)
    else:
        st.info("非试验区域图缺少必要字段：日期/商品id/非试验区域商家供货斤单价/非试验区域抽佣率")

    # 图2：试验区域供货价（城市分组柱状图）
    st.markdown("**图2：试验区域商家供货斤单价（按日期分组的城市柱状图）**")
    trial_supply_col = "试验区域商家供货斤单价"
    if (
        not trial_region_price.empty
        and {"日期", "商品id", "区域全称", trial_supply_col}.issubset(trial_region_price.columns)
    ):
        tr = trial_region_price.copy()
        tr["日期"] = pd.to_datetime(tr["日期"], errors="coerce")
        tr["商品id"] = pd.to_numeric(tr["商品id"], errors="coerce")
        tr = tr[tr["商品id"] == selected_sku].copy()
        tr["城市名称"] = tr["区域全称"].apply(_extract_city_from_region_full_name)
        tr[trial_supply_col] = pd.to_numeric(tr[trial_supply_col], errors="coerce")
        tr = _aggregate_trial_city_daily(tr, trial_supply_col)

        if tr.empty:
            st.info("该商品暂无试验区域供货价数据")
        else:
            fig_trial_supply = go.Figure()
            for city in sorted([c for c in tr["城市名称"].unique().tolist() if c]):
                city_df = tr[tr["城市名称"] == city].sort_values("日期")
                city_text = city_df[trial_supply_col].map(
                    lambda x: "" if pd.isna(x) else f"{x:.2f}"
                )
                fig_trial_supply.add_trace(
                    go.Bar(
                        x=city_df["日期"],
                        y=city_df[trial_supply_col],
                        name=city,
                        text=city_text,
                        textposition="outside",
                        cliponaxis=False,
                    )
                )
            fig_trial_supply.update_layout(
                height=360,
                margin=dict(l=20, r=20, t=30, b=20),
                barmode="group",
            )
            fig_trial_supply.update_xaxes(title_text="日期", tickformat="%Y-%m-%d")
            fig_trial_supply.update_yaxes(title_text="商家供货斤单价", rangemode="tozero")
            st.plotly_chart(fig_trial_supply, use_container_width=True)
    else:
        st.info("试验区域供货价图缺少必要字段：日期/商品id/区域全称/试验区域商家供货斤单价")

    # 图3：试验区域抽佣率（城市分组柱状图）
    st.markdown("**图3：试验区域抽佣率（按日期分组的城市柱状图）**")
    trial_rate_col = "抽佣率"
    if (
        not trial_region_price.empty
        and {"日期", "商品id", "区域全称", trial_rate_col}.issubset(trial_region_price.columns)
    ):
        trr = trial_region_price.copy()
        trr["日期"] = pd.to_datetime(trr["日期"], errors="coerce")
        trr["商品id"] = pd.to_numeric(trr["商品id"], errors="coerce")
        trr = trr[trr["商品id"] == selected_sku].copy()
        trr["城市名称"] = trr["区域全称"].apply(_extract_city_from_region_full_name)
        trr[trial_rate_col] = pd.to_numeric(trr[trial_rate_col], errors="coerce")
        trr = _aggregate_trial_city_daily(trr, trial_rate_col)

        if trr.empty:
            st.info("该商品暂无试验区域抽佣率数据")
        else:
            fig_trial_rate = go.Figure()
            for city in sorted([c for c in trr["城市名称"].unique().tolist() if c]):
                city_df = trr[trr["城市名称"] == city].sort_values("日期")
                city_text = city_df[trial_rate_col].map(
                    lambda x: "" if pd.isna(x) else f"{x:.2%}"
                )
                fig_trial_rate.add_trace(
                    go.Bar(
                        x=city_df["日期"],
                        y=city_df[trial_rate_col],
                        name=city,
                        text=city_text,
                        textposition="outside",
                        cliponaxis=False,
                    )
                )
            fig_trial_rate.update_layout(
                height=360,
                margin=dict(l=20, r=20, t=30, b=20),
                barmode="group",
            )
            fig_trial_rate.update_xaxes(title_text="日期", tickformat="%Y-%m-%d")
            fig_trial_rate.update_yaxes(title_text="抽佣率", tickformat=".2%", rangemode="tozero")
            st.plotly_chart(fig_trial_rate, use_container_width=True)
    else:
        st.info("试验区域抽佣率图缺少必要字段：日期/商品id/区域全称/抽佣率")

    # 图4：试验区域平台销售斤单价（城市分组柱状图）
    st.markdown("**图4：试验区域平台销售斤单价（按日期分组的城市柱状图）**")
    trial_platform_col = "试验区域平台销售斤单价"
    if (
        not trial_region_price.empty
        and {"日期", "商品id", "区域全称", trial_platform_col}.issubset(trial_region_price.columns)
    ):
        trp = trial_region_price.copy()
        trp["日期"] = pd.to_datetime(trp["日期"], errors="coerce")
        trp["商品id"] = pd.to_numeric(trp["商品id"], errors="coerce")
        trp = trp[trp["商品id"] == selected_sku].copy()
        trp["城市名称"] = trp["区域全称"].apply(_extract_city_from_region_full_name)
        trp[trial_platform_col] = pd.to_numeric(trp[trial_platform_col], errors="coerce")
        trp = _aggregate_trial_city_daily(trp, trial_platform_col)

        if trp.empty:
            st.info("该商品暂无试验区域平台销售斤单价数据")
        else:
            fig_trial_platform = go.Figure()
            for city in sorted([c for c in trp["城市名称"].unique().tolist() if c]):
                city_df = trp[trp["城市名称"] == city].sort_values("日期")
                city_text = city_df[trial_platform_col].map(
                    lambda x: "" if pd.isna(x) else f"{x:.2f}"
                )
                fig_trial_platform.add_trace(
                    go.Bar(
                        x=city_df["日期"],
                        y=city_df[trial_platform_col],
                        name=city,
                        text=city_text,
                        textposition="outside",
                        cliponaxis=False,
                    )
                )
            fig_trial_platform.update_layout(
                height=360,
                margin=dict(l=20, r=20, t=30, b=20),
                barmode="group",
            )
            fig_trial_platform.update_xaxes(title_text="日期", tickformat="%Y-%m-%d")
            fig_trial_platform.update_yaxes(title_text="平台销售斤单价", rangemode="tozero")
            st.plotly_chart(fig_trial_platform, use_container_width=True)
    else:
        st.info("试验区域平台销售斤单价图缺少必要字段：日期/商品id/区域全称/试验区域平台销售斤单价")

    # 当日可售卖标记（字段名沿用是否当日上架）
    if (
        not product_info.empty
        and {"商品id", "是否当日上架"}.issubset(product_info.columns)
    ):
        marker_df = product_info.copy()
        marker_df["商品id"] = pd.to_numeric(marker_df["商品id"], errors="coerce")
        marker_df = marker_df[marker_df["商品id"] == selected_sku]
        sellable_today = marker_df[
            pd.to_numeric(marker_df["是否当日上架"], errors="coerce") == 1
        ]
        if not sellable_today.empty:
            st.info(
                f"当前商品发现 {len(sellable_today)} 条当日可售卖标记记录（是否当日上架=1）"
            )
