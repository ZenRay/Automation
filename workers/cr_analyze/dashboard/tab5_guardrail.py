# coding:utf8
"""Tab 5: 护栏预警"""

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px

from workers.cr_analyze.config import ALERT_THRESHOLDS
from .components import render_alert_badge


def render(data: dict[str, pd.DataFrame]):
    wide = data.get("agg_wide_table", pd.DataFrame())

    if wide.empty:
        st.info("暂无聚合宽表数据")
        return

    effect = wide[wide["stage"] == "生效期"].copy()
    baseline = wide[wide["stage"] == "摸底期"].copy()

    # 方案B：生效期为空时，展示摸底期实际监控（不做WoW预警）
    if effect.empty:
        st.subheader("摸底期实际监控（不触发预警）")
        if baseline.empty:
            st.info("暂无摸底期数据")
            return

        monitor_cols = [
            c
            for c in [
                "city_unit",
                "sku_id",
                "order_count",
                "active_store_count",
                "stockout_num",
                "gmv",
                "commission_amount",
            ]
            if c in baseline.columns
        ]
        st.caption("当前阶段为摸底期，展示实际指标，不计算WoW告警。")
        st.dataframe(baseline[monitor_cols], use_container_width=True)
        return

    st.subheader("城市 × SKU 告警状态表")

    if "stage_week" not in effect.columns:
        st.info("生效期数据缺少 stage_week 列")
        return

    # 计算试验周期 WoW（按 stage_week 顺序，不要求完整自然周）
    effect = effect.sort_values(["city_unit", "sku_id", "stage_week"])

    alert_rows = []
    thresholds = ALERT_THRESHOLDS.get("生效期", {})
    order_yellow = thresholds.get("order_count_wow_yellow", -0.10)
    order_red = thresholds.get("order_count_wow_red", -0.15)
    store_yellow = thresholds.get("active_store_count_wow_yellow", -0.05)
    store_red = thresholds.get("active_store_count_wow_red", -0.10)

    for (city, sku), group in effect.groupby(["city_unit", "sku_id"]):
        group = group.sort_values("stage_week")
        weeks = group["stage_week"].tolist()

        for i, week in enumerate(weeks):
            row_data = group[group["stage_week"] == week].iloc[0]
            is_complete = row_data.get("is_complete_week", True)

            order_wow = np.nan
            store_wow = np.nan
            alert = "GREEN"

            if i > 0:
                prev_week = weeks[i - 1]
                prev = group[group["stage_week"] == prev_week].iloc[0]

                if "order_count" in group.columns:
                    prev_val = prev.get("order_count", 0)
                    curr_val = row_data.get("order_count", 0)
                    if prev_val > 0:
                        order_wow = (curr_val - prev_val) / prev_val

                if "active_store_count" in group.columns:
                    prev_val = prev.get("active_store_count", 0)
                    curr_val = row_data.get("active_store_count", 0)
                    if prev_val > 0:
                        store_wow = (curr_val - prev_val) / prev_val

                # 判定告警等级
                if not np.isnan(order_wow) and order_wow < order_red:
                    alert = "RED"
                elif not np.isnan(order_wow) and order_wow < order_yellow:
                    alert = "YELLOW"

                if not np.isnan(store_wow) and store_wow < store_red:
                    alert = "RED"
                elif (
                    alert != "RED"
                    and not np.isnan(store_wow)
                    and store_wow < store_yellow
                ):
                    alert = "YELLOW"

            alert_rows.append(
                {
                    "stage_week": week,
                    "city_unit": city,
                    "sku_id": sku,
                    "is_complete_week": is_complete,
                    "order_count": row_data.get("order_count", np.nan),
                    "active_store_count": row_data.get("active_store_count", np.nan),
                    "order_count_wow": order_wow,
                    "store_count_wow": store_wow,
                    "alert_level": alert,
                }
            )

    if alert_rows:
        alert_df = pd.DataFrame(alert_rows)

        # 格式化显示
        display_df = alert_df.copy()
        display_df["alert_badge"] = display_df["alert_level"].apply(
            lambda x: render_alert_badge(x) if x != "N/A" else "⬜"
        )

        # WoW 按试验周期周序列计算；首周无上周对照显示为 “—”
        for col in ["order_count_wow", "store_count_wow"]:
            if col in display_df.columns:
                display_df[col] = display_df.apply(
                    lambda r: f"{r[col]:.1%}" if pd.notna(r[col]) else "—",
                    axis=1,
                )

        st.dataframe(
            display_df[
                [
                    "alert_badge",
                    "stage_week",
                    "city_unit",
                    "sku_id",
                    "order_count",
                    "active_store_count",
                    "order_count_wow",
                    "store_count_wow",
                ]
            ],
            use_container_width=True,
        )

        # 统计
        red_count = len(alert_df[alert_df["alert_level"] == "RED"])
        yellow_count = len(alert_df[alert_df["alert_level"] == "YELLOW"])
        green_count = len(alert_df[alert_df["alert_level"] == "GREEN"])

        cols = st.columns(3)
        cols[0].metric("🟢 正常", green_count)
        cols[1].metric("🟡 预警", yellow_count)
        cols[2].metric("🔴 告警", red_count)
    else:
        st.info("无法计算环比数据")

    # stockout 趋势
    st.subheader("缺货数量趋势")
    if "stockout_num" in effect.columns:
        stockout = effect.groupby("stage_week")["stockout_num"].sum().reset_index()
        if not stockout.empty:
            fig = px.line(
                stockout,
                x="stage_week",
                y="stockout_num",
                title="生效期缺货数量趋势",
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("暂无缺货数据")

    # 告警阈值说明
    st.subheader("告警阈值参考")
    threshold_rows = []
    for stage, metrics in ALERT_THRESHOLDS.items():
        for metric_name, value in metrics.items():
            threshold_rows.append(
                {
                    "阶段": stage,
                    "指标": metric_name,
                    "阈值": (
                        f"{value:.0%}"
                        if "wow" in metric_name or "deviation" in metric_name
                        else f"{value:.0%}"
                    ),
                }
            )
    st.dataframe(pd.DataFrame(threshold_rows), use_container_width=True)
