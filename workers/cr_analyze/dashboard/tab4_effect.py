# coding:utf8
"""Tab 4: 效应分析 (B/C/D/E 子视图)"""

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px


METRIC_LABEL_TO_COL = {
    "抽佣金额": "commission_amount",
    "货值": "gmv",
    "订单数": "order_count",
    "实际抽佣率": "commission_rate",
    "下单门店数": "active_store_count",
}


def _available_metric_labels(df: pd.DataFrame, labels: list[str]) -> list[str]:
    return [l for l in labels if METRIC_LABEL_TO_COL.get(l) in df.columns]


def _safe_cols(df: pd.DataFrame, cols: list[str]) -> list[str]:
    return [c for c in cols if c in df.columns]


def _aggregate_baseline_city_sku(baseline: pd.DataFrame, metric: str) -> pd.DataFrame:
    """将摸底期聚合为 城市×SKU 唯一行，避免隐藏维度导致重复显示。"""
    if baseline.empty or not {"city_unit", "sku_id"}.issubset(baseline.columns):
        return pd.DataFrame()

    agg_df = baseline.copy()
    group_cols = ["city_unit", "sku_id"]

    if metric == "commission_rate":
        if {"commission_amount", "gmv"}.issubset(agg_df.columns):
            out = (
                agg_df.groupby(group_cols, dropna=False)[["commission_amount", "gmv"]]
                .sum()
                .reset_index()
            )
            out[metric] = np.where(out["gmv"] > 0, out["commission_amount"] / out["gmv"], np.nan)
        else:
            out = (
                agg_df.groupby(group_cols, dropna=False)[metric]
                .mean()
                .reset_index()
            )
    elif metric in {"commission_amount", "gmv", "order_count", "active_store_count"}:
        out = (
            agg_df.groupby(group_cols, dropna=False)[metric]
            .sum()
            .reset_index()
        )
    else:
        out = (
            agg_df.groupby(group_cols, dropna=False)[metric]
            .mean()
            .reset_index()
        )

    if "trading_days" in agg_df.columns:
        td = agg_df.groupby(group_cols, dropna=False)["trading_days"].max().reset_index()
        out = out.merge(td, on=group_cols, how="left")

    return out


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

    labels = _available_metric_labels(wide, ["抽佣金额", "货值", "订单数", "实际抽佣率"])
    if not labels:
        st.info("暂无可用指标")
        return

    metric_label = st.selectbox("指标", labels, key="tab4_city_metric")
    metric = METRIC_LABEL_TO_COL[metric_label]
    if metric not in wide.columns:
        st.warning(f"指标 {metric_label} 不可用")
        return

    # 摸底期基准点
    if (
        not baseline.empty
        and "city_unit" in baseline.columns
        and "sku_id" in baseline.columns
    ):
        st.write("**摸底期基准**")
        baseline_show = _aggregate_baseline_city_sku(baseline, metric)
        show_cols = _safe_cols(baseline_show, ["city_unit", "sku_id", metric, "trading_days"])
        if not show_cols:
            st.info("摸底期基准缺少必要字段")
            return
        zh_rename = {
            "city_unit": "城市",
            "sku_id": "商品ID",
            metric: metric_label,
            "trading_days": "交易天数",
        }
        st.dataframe(
            baseline_show[show_cols].dropna(how="all").rename(columns=zh_rename),
            use_container_width=True,
        )

    # 生效期趋势
    if not effect.empty and "stage_week" in effect.columns:
        st.write("**生效期趋势**")
        base_cols = ["stage_week", "city_unit", "sku_id", metric]
        extra_cols = [c for c in ["trading_days", "is_complete_week"] if c in effect.columns]
        fig_data = effect[base_cols + extra_cols].dropna(subset=base_cols)
        if not fig_data.empty:
            fig_data["label"] = (
                fig_data["city_unit"].astype(str)
                + " / "
                + fig_data["sku_id"].astype(str)
            )

            if "is_complete_week" in fig_data.columns and "trading_days" in fig_data.columns:
                fig_data["point_text"] = np.where(
                    fig_data["is_complete_week"] == False,
                    "⚠ " + fig_data["trading_days"].fillna(0).astype(int).astype(str) + "/7天",
                    "",
                )
            elif "trading_days" in fig_data.columns:
                fig_data["point_text"] = fig_data["trading_days"].fillna(0).astype(int).astype(str) + "/7天"
            else:
                fig_data["point_text"] = ""

            fig = px.line(
                fig_data,
                x="stage_week",
                y=metric,
                color="label",
                title=f"生效期{metric_label}趋势",
                labels={"stage_week": "生效周", metric: metric_label, "label": "城市/SKU"},
                hover_data={
                    "stage_week": True,
                    "city_unit": True,
                    "sku_id": True,
                    metric: True,
                    "trading_days": True if "trading_days" in fig_data.columns else False,
                    "is_complete_week": True if "is_complete_week" in fig_data.columns else False,
                },
            )
            fig.update_traces(text=fig_data["point_text"], textposition="top center")
            fig.update_yaxes(title_text=metric_label)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("生效期趋势缺少必要字段或为空")

        # 残缺周标注
        if "is_complete_week" in effect.columns:
            incomplete = effect[effect["is_complete_week"] == False]
            if not incomplete.empty:
                st.warning(f"发现 {len(incomplete)} 个残缺周数据点")
    else:
        st.info("暂无生效期周趋势数据")


def _render_group_agg(wide: pd.DataFrame):
    """Sub-view C: 分组聚合"""
    st.subheader("分组聚合趋势")

    labels = _available_metric_labels(wide, ["抽佣金额", "货值", "订单数"])
    if not labels:
        st.info("暂无可用指标")
        return

    metric_label = st.selectbox("指标", labels, key="tab4_group_metric")
    metric = METRIC_LABEL_TO_COL[metric_label]
    if metric not in wide.columns:
        st.warning(f"指标 {metric_label} 不可用")
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
        period_count = combined["period"].nunique(dropna=True)
        if period_count <= 1:
            # 只有摸底期单点时，线图不可读，退化为分组柱图确保可见。
            one_period = (
                combined.groupby("trial_group", dropna=False)[metric]
                .mean()
                .reset_index()
            )
            fig = px.bar(
                one_period,
                x="trial_group",
                y=metric,
                title=f"各试验组{metric_label}（摸底期）",
                labels={"trial_group": "试验组", metric: metric_label},
            )
            fig.update_traces(text=one_period[metric].round(2), textposition="outside", cliponaxis=False)
            fig.update_yaxes(title_text=metric_label)
            st.plotly_chart(fig, use_container_width=True)
            st.info("当前仅有摸底期数据，生效期周趋势将在 stage_week 数据就绪后展示。")
        else:
            fig = px.line(
                combined,
                x="period",
                y=metric,
                color="trial_group",
                title=f"各试验组{metric_label}均值趋势",
                markers=True,
                labels={"period": "阶段/周", metric: metric_label, "trial_group": "试验组"},
            )
            fig.update_xaxes(type="category")
            fig.update_yaxes(title_text=metric_label)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("暂无分组聚合趋势数据")


def _render_sku_comparison(wide: pd.DataFrame):
    """Sub-view D: SKU 对比"""
    st.subheader("SKU 对比")

    effect = wide[wide["stage"] == "生效期"].copy()
    baseline = wide[wide["stage"] == "摸底期"].copy()

    target = effect if not effect.empty else baseline
    if target.empty or "sku_id" not in target.columns:
        st.info("暂无 SKU 对比数据")
        return

    labels = _available_metric_labels(target, ["抽佣金额", "货值", "实际抽佣率"])
    if not labels:
        st.info("暂无可用指标")
        return

    metric_label = st.selectbox("指标", labels, key="tab4_sku_metric")
    metric = METRIC_LABEL_TO_COL[metric_label]
    if metric not in target.columns:
        return

    # 按规格仅展示 3 个目标 SKU。
    target_skus = [10184690, 20519020, 20588413]
    target = target[target["sku_id"].isin(target_skus)].copy()
    if target.empty:
        st.info("当前阶段暂无目标SKU数据")
        return

    if "trial_group" in target.columns:
        bar_df = target.groupby(["trial_group", "sku_id"])[metric].mean().reset_index()
        bar_df["sku_id"] = bar_df["sku_id"].astype(str)
        fig = px.bar(
            bar_df,
            x="trial_group",
            y=metric,
            color="sku_id",
            barmode="group",
            title=f"各试验组SKU{metric_label}对比",
            labels={"trial_group": "试验组", "sku_id": "商品ID"},
        )
        fig.update_traces(text=bar_df[metric].round(2), textposition="outside", cliponaxis=False)
        fig.update_xaxes(type="category")
        fig.update_yaxes(title_text=metric_label)
        st.plotly_chart(fig, use_container_width=True)
    else:
        bar_df = target.groupby(["sku_id"])[metric].mean().reset_index()
        bar_df["sku_id"] = bar_df["sku_id"].astype(str)
        fig = px.bar(
            bar_df,
            x="sku_id",
            y=metric,
            title=f"SKU {metric_label} 对比",
            labels={"sku_id": "商品ID", metric: metric_label},
        )
        fig.update_traces(text=bar_df[metric].round(2), textposition="outside", cliponaxis=False)
        fig.update_yaxes(title_text=metric_label)
        st.plotly_chart(fig, use_container_width=True)


def _render_origin_comparison(wide: pd.DataFrame):
    """Sub-view E: 产地对比"""
    st.subheader("产地对比")

    if "sku_id" not in wide.columns:
        st.info("暂无产地对比所需 SKU 字段")
        return

    df = wide.copy()

    # 云南: 10184690 + 20519020; 广西: 20588413
    yunnan_skus = [10184690, 20519020]
    guangxi_skus = [20588413]

    yunnan = df[df["sku_id"].isin(yunnan_skus)]
    guangxi = df[df["sku_id"].isin(guangxi_skus)]

    metric_labels = ["抽佣金额", "实际抽佣率"]
    available_labels = _available_metric_labels(df, metric_labels)

    if available_labels:
        rows = []
        for label in available_labels:
            m = METRIC_LABEL_TO_COL[label]
            rows.append(
                {
                    "指标": label,
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

        # 柱状图对比（按 spec: 抽佣金额、实际抽佣率）
        for label in ["抽佣金额", "实际抽佣率"]:
            m = METRIC_LABEL_TO_COL[label]
            if m in df.columns:
                if m == "commission_rate":
                    y_val = yunnan[m].mean()
                    g_val = guangxi[m].mean()
                else:
                    y_val = yunnan[m].sum()
                    g_val = guangxi[m].sum()
                compare_df = pd.DataFrame(
                    {
                        "产地": ["云南", "广西"],
                        m: [y_val, g_val],
                    }
                )
                fig = px.bar(compare_df, x="产地", y=m, title=f"产地{label}对比", labels={"产地": "产地", m: label})
                fig.update_traces(text=compare_df[m].round(4), textposition="outside", cliponaxis=False)
                fig.update_yaxes(title_text=label)
                st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("暂无可用于产地对比的指标")
