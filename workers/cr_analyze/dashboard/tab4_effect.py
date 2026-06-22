# coding:utf8
"""Tab 4: 效应分析 (B/C/D/E 子视图)"""

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px


def render(data: dict[str, pd.DataFrame]):
    wide = data.get("agg_wide_table", pd.DataFrame())

    if wide.empty:
        st.info("暂无聚合宽表数据")
        return

    sub_view = st.radio(
        "选择视图",
        ["城市视图 (B)", "分组视图 (C)", "SKU 对比 (D)", "产地对比 (E)"],
        horizontal=True,
    )

    if sub_view.startswith("城市"):
        _render_city_trends(wide)
    elif sub_view.startswith("分组"):
        _render_group_agg(wide)
    elif sub_view.startswith("SKU"):
        _render_sku_comparison(wide)
    else:
        _render_origin_comparison(wide)


def _render_city_trends(wide: pd.DataFrame):
    """Sub-view B: 城市单元趋势"""
    st.subheader("城市单元趋势")

    # 摸底期基准 + 生效期按周
    baseline = wide[wide["stage"] == "摸底期"].copy()
    effect = wide[wide["stage"] == "生效期"].copy()

    metric = st.selectbox(
        "指标", ["commission_amount", "gmv", "order_count", "commission_rate"]
    )
    if metric not in wide.columns:
        st.warning(f"指标 {metric} 不可用")
        return

    # 摸底期基准点
    if (
        not baseline.empty
        and "city_unit" in baseline.columns
        and "sku_id" in baseline.columns
    ):
        st.write("**摸底期基准**")
        st.dataframe(
            baseline[["city_unit", "sku_id", metric, "trading_days"]].dropna(),
            use_container_width=True,
        )

    # 生效期趋势
    if not effect.empty and "stage_week" in effect.columns:
        st.write("**生效期趋势**")
        fig_data = effect[["stage_week", "city_unit", "sku_id", metric]].dropna()
        if not fig_data.empty:
            fig_data["label"] = (
                fig_data["city_unit"].astype(str)
                + " / "
                + fig_data["sku_id"].astype(str)
            )

            fig = px.line(
                fig_data,
                x="stage_week",
                y=metric,
                color="label",
                title=f"生效期 {metric} 趋势",
            )
            st.plotly_chart(fig, use_container_width=True)

        # 残缺周标注
        incomplete = effect[effect.get("is_complete_week", True) == False]
        if not incomplete.empty:
            st.warning(f"⚠️ 发现 {len(incomplete)} 个残缺周数据点")


def _render_group_agg(wide: pd.DataFrame):
    """Sub-view C: 分组聚合"""
    st.subheader("分组聚合趋势")

    metric = st.selectbox(
        "指标",
        ["commission_amount", "gmv", "order_count"],
        key="group_metric",
    )
    if metric not in wide.columns:
        st.warning(f"指标 {metric} 不可用")
        return

    # 摸底期均值
    baseline = wide[wide["stage"] == "摸底期"].copy()
    effect = wide[wide["stage"] == "生效期"].copy()

    frames = []
    if not baseline.empty and "trial_group" in baseline.columns:
        b = baseline.groupby("trial_group")[metric].mean().reset_index()
        b["period"] = "摸底期"
        frames.append(b)

    if (
        not effect.empty
        and "trial_group" in effect.columns
        and "stage_week" in effect.columns
    ):
        e = effect.groupby(["trial_group", "stage_week"])[metric].mean().reset_index()
        e["period"] = e["stage_week"]
        frames.append(e)

    if frames:
        combined = pd.concat(frames, ignore_index=True)
        fig = px.line(
            combined,
            x="period",
            y=metric,
            color="trial_group",
            title=f"各试验组 {metric} 均值趋势",
        )
        st.plotly_chart(fig, use_container_width=True)


def _render_sku_comparison(wide: pd.DataFrame):
    """Sub-view D: SKU 对比"""
    st.subheader("SKU 对比")

    effect = wide[wide["stage"] == "生效期"].copy()
    baseline = wide[wide["stage"] == "摸底期"].copy()

    target = effect if not effect.empty else baseline
    if target.empty or "sku_id" not in target.columns:
        st.info("暂无 SKU 对比数据")
        return

    metric = st.selectbox(
        "指标",
        ["commission_amount", "gmv", "commission_rate"],
        key="sku_metric",
    )
    if metric not in target.columns:
        return

    if "trial_group" in target.columns:
        fig = px.bar(
            target.groupby(["trial_group", "sku_id"])[metric].mean().reset_index(),
            x="sku_id",
            y=metric,
            color="trial_group",
            barmode="group",
            title=f"各试验组 SKU {metric} 对比",
        )
        st.plotly_chart(fig, use_container_width=True)


def _render_origin_comparison(wide: pd.DataFrame):
    """Sub-view E: 产地对比"""
    st.subheader("产地对比")

    if "sku_origin" not in wide.columns or "sku_id" not in wide.columns:
        st.info("暂无产地维度数据")
        return

    df = wide.copy()

    # 云南: 10184690 + 20519020; 广西: 20588413
    yunnan_skus = [10184690, 20519020]
    guangxi_skus = [20588413]

    yunnan = df[df["sku_id"].isin(yunnan_skus)]
    guangxi = df[df["sku_id"].isin(guangxi_skus)]

    metrics = ["commission_amount", "commission_rate", "gmv"]
    available = [m for m in metrics if m in df.columns]

    if available:
        rows = []
        for m in available:
            rows.append(
                {
                    "指标": m,
                    "云南 (10184690+20519020)": (
                        yunnan[m].sum()
                        if m == "commission_amount" or m == "gmv"
                        else yunnan[m].mean()
                    ),
                    "广西 (20588413)": (
                        guangxi[m].sum()
                        if m == "commission_amount" or m == "gmv"
                        else guangxi[m].mean()
                    ),
                }
            )
        st.dataframe(pd.DataFrame(rows), use_container_width=True)

        # 柱状图对比
        for m in ["commission_amount", "gmv"]:
            if m in df.columns:
                y_val = yunnan[m].sum()
                g_val = guangxi[m].sum()
                compare_df = pd.DataFrame(
                    {
                        "产地": ["云南", "广西"],
                        m: [y_val, g_val],
                    }
                )
                fig = px.bar(compare_df, x="产地", y=m, title=f"产地 {m} 对比")
                st.plotly_chart(fig, use_container_width=True)
