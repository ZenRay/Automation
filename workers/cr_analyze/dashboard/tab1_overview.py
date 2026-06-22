# coding:utf8
"""Tab 1: 试验总览"""

import pandas as pd
import streamlit as st

from workers.cr_analyze.config import TRIAL_PHASE_CONFIG, TARGET_R0_REFERENCE


def render(data: dict[str, pd.DataFrame]):
    trial_group = data.get("conf_trial_group", pd.DataFrame())
    period_rate = data.get("conf_trial_period_rate", pd.DataFrame())

    # 当前阶段卡片
    st.subheader("试验当前阶段")
    from datetime import date

    # 从数据中推导参考日期：取 period_rate 中最新的结束日期
    ref_date = date.today()
    if not period_rate.empty:
        for col in ["试验结束日期", "试验起始日期"]:
            if col in period_rate.columns:
                parsed = pd.to_datetime(period_rate[col], errors="coerce").dt.date
                if not parsed.dropna().empty:
                    ref_date = max(parsed.dropna().max(), ref_date)
                    break

    current_stage = "未配置"
    stage_start = stage_end = None
    if not period_rate.empty:
        period_rate_dates = period_rate.copy()
        for col in ["试验起始日期", "试验结束日期"]:
            if col in period_rate_dates.columns:
                period_rate_dates[col] = pd.to_datetime(
                    period_rate_dates[col], errors="coerce"
                ).dt.date

        for _, row in period_rate_dates.iterrows():
            s, e = row.get("试验起始日期"), row.get("试验结束日期")
            if pd.notna(s) and pd.notna(e) and s <= ref_date <= e:
                current_stage = row.get("试验阶段", "未知")
                stage_start, stage_end = s, e
                break

    cols = st.columns(3)
    cols[0].metric("当前阶段", current_stage)
    cols[1].metric("起始日期", str(stage_start) if stage_start else "-")
    cols[2].metric("结束日期", str(stage_end) if stage_end else "-")

    # 8 城市分组配置表
    st.subheader("城市分组配置")
    if not trial_group.empty:
        display_df = trial_group.copy()
        # 只保留 CITY 类型
        if "区域类型" in display_df.columns:
            display_df = display_df[display_df["区域类型"] == "CITY"]
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("暂无试验分组配置数据")

    # 关键时间节点
    st.subheader("关键时间节点")
    dragon_boat = TRIAL_PHASE_CONFIG.get("dragon_boat_dates", [])
    if dragon_boat:
        st.info(
            f"端午节期间: {dragon_boat[0]} ~ {dragon_boat[-1]} "
            f"(共 {len(dragon_boat)} 天，摸底期内排除)"
        )

    # SKU 清单
    st.subheader("试验 SKU 清单")
    from workers.cr_analyze.config import TARGET_SKU_IDS

    sku_df = pd.DataFrame({"商品id": TARGET_SKU_IDS})
    product_info = data.get("conf_product_info", pd.DataFrame())
    if not product_info.empty and "商品id" in product_info.columns:
        sku_names = product_info[product_info["商品id"].isin(TARGET_SKU_IDS)][
            ["商品id", "商品名称", "产地"]
        ].drop_duplicates()
        if not sku_names.empty:
            sku_df = sku_names
    st.dataframe(sku_df, use_container_width=True)
