# coding:utf8
"""Tab 3: 归一化进度"""

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px


def _aggregate_norm_view(df: pd.DataFrame, has_date_col: bool) -> pd.DataFrame:
    """聚合到归一化看板所需粒度，避免 SKU 维度导致重复和颜色冲突。"""
    if df.empty:
        return df

    base = df.copy()
    group_cols = ["city_unit", "region_type"]
    if has_date_col and "日期" in base.columns:
        group_cols = ["日期"] + group_cols

    # commission_rate 优先采用加权口径：sum(commission_amount)/sum(gmv)
    has_weight_cols = {"commission_amount", "gmv"}.issubset(base.columns)
    if has_weight_cols:
        agg = (
            base.groupby(group_cols, dropna=False)[["commission_amount", "gmv"]]
            .sum()
            .reset_index()
        )
        agg["commission_rate"] = np.where(
            agg["gmv"] > 0,
            agg["commission_amount"] / agg["gmv"],
            np.nan,
        )
    else:
        agg = (
            base.groupby(group_cols, dropna=False)["commission_rate"]
            .mean()
            .reset_index()
        )

    return agg


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

    # 日期列在部分宽表粒度下可能不存在（如摸底期整体聚合）。
    has_date_col = "日期" in prep.columns
    if has_date_col:
        prep["日期"] = pd.to_datetime(prep["日期"], errors="coerce")

    st.subheader("各城市每日 r₀ 趋势")

    # 目标线
    target_self = 0.075  # 自营
    target_agent = 0.046  # 代理人

    if "commission_rate" in prep.columns and "city_unit" in prep.columns:
        prep_view = _aggregate_norm_view(prep, has_date_col=has_date_col)
        cols = ["city_unit", "region_type", "commission_rate"]
        if has_date_col:
            cols = ["日期"] + cols
        fig_data = prep_view[[c for c in cols if c in prep_view.columns]].dropna(
            subset=[c for c in ["city_unit", "region_type", "commission_rate"] if c in prep.columns]
        )
        if not fig_data.empty:
            fig_data["label"] = (
                fig_data["city_unit"].astype(str)
                + " ("
                + fig_data["region_type"].astype(str)
                + ")"
            )

            if has_date_col and "日期" in fig_data.columns:
                fig = px.line(
                    fig_data,
                    x="日期",
                    y="commission_rate",
                    color="label",
                    title="各城市 r₀ 日趋势",
                    labels={"commission_rate": "实际抽佣率(r₀)", "日期": "日期"},
                )
            else:
                agg = (
                    fig_data.groupby(["city_unit", "region_type", "label"], dropna=False)["commission_rate"]
                    .mean()
                    .reset_index()
                )
                fig = px.bar(
                    agg,
                    x="label",
                    y="commission_rate",
                    title="各城市 r₀ 当前水平（无日期维度）",
                    labels={"commission_rate": "实际抽佣率(r₀)", "label": "城市 (区域类型)"},
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
            if not has_date_col:
                st.info("当前聚合宽表不包含“日期”列，已按城市展示 r₀ 当前水平。")

    # 偏差柱状图
    st.subheader("r₀ 偏差 (实际 - 目标)")
    if "commission_rate" in prep.columns and "region_type" in prep.columns:
        dev_data = _aggregate_norm_view(prep, has_date_col=has_date_col)
        dev_data["target_r0"] = dev_data["region_type"].apply(
            lambda x: target_self if "自营" in str(x) else target_agent
        )
        dev_data["r0_deviation"] = dev_data["commission_rate"] - dev_data["target_r0"]

        latest_date_text = "当前阶段"
        if "日期" in dev_data.columns:
            latest_date = dev_data["日期"].max()
            latest = dev_data[dev_data["日期"] == latest_date]
            latest_date_text = str(latest_date.date()) if pd.notna(latest_date) else "当前阶段"
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
            latest["状态"] = latest["r0_deviation"].apply(
                lambda x: "超阈值" if abs(x) > 0.01 else "正常"
            )
            latest = latest.sort_values("label")

            fig2 = px.bar(
                latest,
                x="label",
                y="r0_deviation",
                color="状态",
                color_discrete_map={"超阈值": "#ff4444", "正常": "#44bb44"},
                title=f"r₀ 偏差 ({latest_date_text})",
                labels={"r0_deviation": "偏差值", "label": "城市 (区域类型)", "状态": "告警状态"},
            )
            fig2.add_hline(y=0.01, line_dash="dash", line_color="orange")
            fig2.add_hline(y=-0.01, line_dash="dash", line_color="orange")
            st.plotly_chart(fig2, use_container_width=True)

    # 达标汇总表
    st.subheader("归一化达标汇总")
    if "commission_rate" in prep.columns:
        summary = _aggregate_norm_view(prep, has_date_col=has_date_col)
        if "region_type" not in summary.columns:
            st.info("归一化汇总缺少 region_type 字段")
            return

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
        zh_rename = {
            "city_unit": "城市",
            "region_type": "区域类型",
            "commission_rate": "实际抽佣率(r₀)",
            "target_r0": "目标r₀",
            "deviation": "偏差",
            "status": "状态",
        }
        st.dataframe(summary[display_cols].rename(columns=zh_rename), use_container_width=True)
