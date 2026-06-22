# coding:utf8
"""Tab 3: 归一化进度"""

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px


def render(data: dict[str, pd.DataFrame]):
    wide = data.get("agg_wide_table", pd.DataFrame())

    if wide.empty:
        st.info("暂无聚合宽表数据，请先运行数据管道")
        return

    # 筛选归一化预备期数据
    prep = wide[wide["stage"] == "归一化预备期"].copy()
    if prep.empty:
        st.info("暂无归一化预备期数据")
        # 尝试使用摸底期数据
        prep = wide[wide["stage"] == "摸底期"].copy()
        if prep.empty:
            return

    # 确保日期列存在
    if "日期" in prep.columns:
        prep["日期"] = pd.to_datetime(prep["日期"], errors="coerce")

    st.subheader("各城市每日 r₀ 趋势")

    # 目标线
    target_self = 0.075  # 自营
    target_agent = 0.046  # 代理人

    if "commission_rate" in prep.columns and "city_unit" in prep.columns:
        fig_data = prep[
            ["日期", "city_unit", "region_type", "commission_rate"]
        ].dropna()
        if not fig_data.empty:
            fig_data["label"] = (
                fig_data["city_unit"].astype(str)
                + " ("
                + fig_data["region_type"].astype(str)
                + ")"
            )

            fig = px.line(
                fig_data,
                x="日期",
                y="commission_rate",
                color="label",
                title="各城市 r₀ 日趋势",
                labels={"commission_rate": "实际 r₀", "日期": "日期"},
            )
            fig.add_hline(
                y=target_self,
                line_dash="dash",
                line_color="blue",
                annotation_text=f"自营目标 {target_self:.1%}",
            )
            fig.add_hline(
                y=target_agent,
                line_dash="dash",
                line_color="red",
                annotation_text=f"代理人目标 {target_agent:.1%}",
            )
            st.plotly_chart(fig, use_container_width=True)

    # 偏差柱状图
    st.subheader("r₀ 偏差 (实际 - 目标)")
    if "commission_rate" in prep.columns and "region_type" in prep.columns:
        dev_data = prep.copy()
        dev_data["target_r0"] = dev_data["region_type"].apply(
            lambda x: target_self if "自营" in str(x) else target_agent
        )
        dev_data["r0_deviation"] = dev_data["commission_rate"] - dev_data["target_r0"]

        if "日期" in dev_data.columns:
            latest_date = dev_data["日期"].max()
            latest = dev_data[dev_data["日期"] == latest_date]
        else:
            latest = dev_data

        if not latest.empty:
            latest = latest.copy()
            latest["label"] = (
                latest.get("city_unit", "").astype(str)
                + " ("
                + latest["region_type"].astype(str)
                + ")"
            )
            latest["color"] = latest["r0_deviation"].apply(
                lambda x: "red" if abs(x) > 0.01 else "green"
            )

            fig2 = px.bar(
                latest,
                x="label",
                y="r0_deviation",
                color="color",
                color_discrete_map={"red": "#ff4444", "green": "#44bb44"},
                title=f"r₀ 偏差 (最新日期: {latest_date})",
                labels={"r0_deviation": "偏差值", "label": "城市 (区域类型)"},
            )
            fig2.add_hline(y=0.01, line_dash="dash", line_color="orange")
            fig2.add_hline(y=-0.01, line_dash="dash", line_color="orange")
            st.plotly_chart(fig2, use_container_width=True)

    # 达标汇总表
    st.subheader("归一化达标汇总")
    if "commission_rate" in prep.columns:
        summary = prep.copy()
        summary["target_r0"] = summary["region_type"].apply(
            lambda x: target_self if "自营" in str(x) else target_agent
        )
        summary["deviation"] = summary["commission_rate"] - summary["target_r0"]
        summary["status"] = summary["deviation"].apply(
            lambda x: "达标" if abs(x) <= 0.01 else ("偏高" if x > 0.01 else "偏低")
        )

        if "日期" in summary.columns:
            latest_date = summary["日期"].max()
            summary = summary[summary["日期"] == latest_date]

        display_cols = [
            "city_unit",
            "region_type",
            "commission_rate",
            "target_r0",
            "deviation",
            "status",
        ]
        display_cols = [c for c in display_cols if c in summary.columns]
        st.dataframe(summary[display_cols], use_container_width=True)
