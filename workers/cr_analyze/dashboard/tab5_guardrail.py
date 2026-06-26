# coding:utf8
"""Tab 5: 护栏预警"""

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
import re

from workers.cr_analyze.config import ALERT_THRESHOLDS
from .components import render_alert_badge


METRIC_LABEL_TO_COL = {
    "下单数量": "ordered_num",
    "货值": "gmv",
    "抽佣金额": "commission_amount",
    "实际抽佣率": "commission_rate",
    "下单门店数": "active_store_count",
    "缺货数量": "stockout_num",
}


def _canonical_sku(v) -> str:
    if pd.isna(v):
        return ""
    try:
        return str(int(float(v)))
    except Exception:
        return str(v).strip()


def _format_metric_value(metric_label: str, value):
    if pd.isna(value):
        return "-"
    if metric_label == "实际抽佣率":
        return f"{value:.2%}"
    if metric_label in {"货值", "抽佣金额"}:
        return f"{value:,.2f}"
    return f"{value:,.0f}"


def _extract_pre_week_num(label: str) -> int | None:
    if not isinstance(label, str):
        return None
    m = re.match(r"^摸底期前(\d+)周$", label)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _cycle_recent_rank(label: str) -> int:
    if label == "摸底期":
        return 0
    n = _extract_pre_week_num(label)
    if n is None:
        return 999
    return n


def _cycle_timeline_rank(label: str) -> int:
    if label == "摸底期":
        return 6
    n = _extract_pre_week_num(label)
    if n is None:
        return 999
    # 时间正序：前5周 -> 前4周 -> ... -> 前1周 -> 摸底期
    return 6 - n


def _build_latest_product_name_map(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    if df.empty or not group_cols or "商品名称" not in df.columns or "日期" not in df.columns:
        return pd.DataFrame(columns=[*group_cols, "商品名称"])

    pick_cols = list(group_cols)
    if "日期" not in pick_cols:
        pick_cols.append("日期")
    sort_cols = list(group_cols)
    if "日期" not in group_cols:
        sort_cols.append("日期")

    latest_name = (
        df[[*pick_cols, "商品名称"]]
        .copy()
        .sort_values(sort_cols, ascending=[True] * len(sort_cols))
        .drop_duplicates(subset=group_cols, keep="last")
    )
    return latest_name[[*group_cols, "商品名称"]]


def _apply_bar_value_labels(fig):
    fig.update_traces(texttemplate="%{y}", textposition="outside", cliponaxis=False)
    return fig


def _season_range_filter(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    if df.empty or date_col not in df.columns:
        return df
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out = out.dropna(subset=[date_col])
    md = out[date_col].dt.month * 100 + out[date_col].dt.day
    return out[(md >= 401) & (md <= 731)]


def _build_attr_combo(df: pd.DataFrame, dims: list[str], combo_col: str = "属性组合") -> pd.DataFrame:
    out = df.copy()
    if not dims:
        out[combo_col] = "整体"
        return out
    out[combo_col] = out[dims].fillna("未知").astype(str).agg(" / ".join, axis=1)
    return out


def _build_city_display(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["省名称"] = out.get("省名称", pd.Series([pd.NA] * len(out)))
    out["市名称"] = out.get("市名称", pd.Series([pd.NA] * len(out)))
    out["city_pair"] = (
        out["省名称"].astype(str).replace("nan", "")
        + "-"
        + out["市名称"].astype(str).replace("nan", "")
    ).str.strip("-")
    return out


def _build_sku_meta(data: dict[str, pd.DataFrame]) -> pd.DataFrame:
    conf_product = data.get("conf_product_info", pd.DataFrame())
    fact = data.get("fact_order_item", pd.DataFrame())

    frames = []
    if not conf_product.empty and "商品id" in conf_product.columns:
        cdf = conf_product[[c for c in ["商品id", "商品名称", "日期"] if c in conf_product.columns]].copy()
        cdf = cdf.rename(columns={"商品id": "sku_id"})
        if "日期" not in cdf.columns:
            cdf["日期"] = pd.NaT
        cdf["_source_priority"] = 0
        frames.append(cdf)
    if not fact.empty and "商品id" in fact.columns:
        fdf = fact[[c for c in ["商品id", "商品名称", "日期"] if c in fact.columns]].copy()
        fdf = fdf.rename(columns={"商品id": "sku_id"})
        if "日期" not in fdf.columns:
            fdf["日期"] = pd.NaT
        fdf["_source_priority"] = 1
        frames.append(fdf)

    if not frames:
        return pd.DataFrame(columns=["sku_id", "商品名称", "sku_key"])

    meta = pd.concat(frames, ignore_index=True)
    meta["sku_key"] = meta["sku_id"].map(_canonical_sku)
    meta["日期"] = pd.to_datetime(meta.get("日期"), errors="coerce")
    meta["商品名称"] = meta.get("商品名称", pd.Series([""] * len(meta))).fillna("").astype(str)
    meta["_name_not_empty"] = meta["商品名称"].str.len() > 0
    meta = meta[meta["sku_key"] != ""]
    meta = meta.sort_values(
        ["sku_key", "日期", "_source_priority", "_name_not_empty"],
        ascending=[True, False, False, False],
    )
    meta = meta.drop_duplicates(subset=["sku_key"], keep="first")
    return meta[["sku_key", "商品名称"]]


def _build_trial_sku_set(data: dict[str, pd.DataFrame]) -> set[str]:
    sku_set: set[str] = set()

    trial_sku_profile = data.get("trial_sku_profile", pd.DataFrame())
    if not trial_sku_profile.empty and "商品id" in trial_sku_profile.columns:
        sku_set |= {
            _canonical_sku(v)
            for v in trial_sku_profile["商品id"].tolist()
            if _canonical_sku(v)
        }

    trial_region_price = data.get("conf_trial_region_price", pd.DataFrame())
    if not trial_region_price.empty and "商品id" in trial_region_price.columns:
        sku_set |= {
            _canonical_sku(v)
            for v in trial_region_price["商品id"].tolist()
            if _canonical_sku(v)
        }

    wide = data.get("agg_wide_table", pd.DataFrame())
    if not wide.empty and "sku_id" in wide.columns:
        sku_set |= {
            _canonical_sku(v)
            for v in wide["sku_id"].tolist()
            if _canonical_sku(v)
        }

    return sku_set


def _sku_label(sku_key: str, sku_name_map: dict[str, str]) -> str:
    name = sku_name_map.get(sku_key, "")
    return f"{sku_key} - {name}" if name else sku_key


def _build_trial_group_map(conf_trial_group: pd.DataFrame) -> pd.DataFrame:
    if conf_trial_group.empty or "市名称" not in conf_trial_group.columns:
        return pd.DataFrame(columns=["市名称", "试验分组", "试验起始日期"])

    tg = conf_trial_group.copy()
    tg["试验起始日期"] = pd.to_datetime(tg.get("试验起始日期"), errors="coerce")
    keep_cols = [c for c in ["市名称", "试验分组", "试验起始日期"] if c in tg.columns]
    tg = tg[keep_cols]
    tg = tg.sort_values(["市名称", "试验起始日期"], ascending=[True, True])
    tg = tg.drop_duplicates(subset=["市名称"], keep="first")
    return tg


def _build_baseline_window(conf_trial_period_rate: pd.DataFrame):
    if conf_trial_period_rate.empty:
        return None, None
    c = conf_trial_period_rate.copy()
    if "试验阶段" in c.columns:
        c = c[c["试验阶段"] == "摸底期"]
    if c.empty:
        return None, None
    c["试验起始日期"] = pd.to_datetime(c.get("试验起始日期"), errors="coerce")
    c["试验结束日期"] = pd.to_datetime(c.get("试验结束日期"), errors="coerce")
    start_date = c["试验起始日期"].min()
    end_date = c["试验结束日期"].max()
    if pd.isna(start_date) or pd.isna(end_date):
        return None, None
    return start_date, end_date


def _build_baseline_monitor_df(
    data: dict[str, pd.DataFrame],
    selected_skus: list[str],
    selected_cities: list[str],
    selected_region_scope: list[str],
    granularity: str,
) -> pd.DataFrame:
    fact = data.get("fact_order_item", pd.DataFrame())
    if fact.empty:
        return pd.DataFrame()

    df = fact.copy()
    df.columns = [str(c).strip() for c in df.columns]
    required = {"日期", "商品id", "市名称", "下单数量", "送达金额", "送达抽佣金额"}
    if not required.issubset(df.columns):
        return pd.DataFrame()

    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["日期"])

    # 仅保留摸底期及摸底期以前
    start_date, end_date = _build_baseline_window(data.get("conf_trial_period_rate", pd.DataFrame()))
    if end_date is not None:
        df = df[df["日期"] <= end_date]

    df["sku_key"] = df["商品id"].map(_canonical_sku)
    if selected_skus:
        df = df[df["sku_key"].isin(selected_skus)]
    if selected_cities:
        df = df[df["市名称"].isin(selected_cities)]

    df["下单数量"] = pd.to_numeric(df["下单数量"], errors="coerce")
    df["送达金额"] = pd.to_numeric(df["送达金额"], errors="coerce")
    df["送达抽佣金额"] = pd.to_numeric(df["送达抽佣金额"], errors="coerce")

    if "店铺id" in df.columns:
        df["店铺id"] = df["店铺id"].astype(str)
    else:
        df["店铺id"] = pd.NA

    if "订单id" in df.columns:
        df["订单id"] = df["订单id"].astype(str)
    else:
        df["订单id"] = pd.NA

    tg = _build_trial_group_map(data.get("conf_trial_group", pd.DataFrame()))
    if not tg.empty:
        df = df.merge(tg[["市名称", "试验分组", "试验起始日期"]], on="市名称", how="left")
    else:
        df["试验分组"] = pd.NA
        df["试验起始日期"] = pd.NaT
    df["试验分组"] = df["试验分组"].fillna("非试验区域")

    trial_groups = {"对照组", "试验组一", "试验组二", "试验组三", "实验组一", "实验组二", "实验组三"}
    selected_region_scope = selected_region_scope or ["试验区域", "非试验区域"]
    if "试验区域" in selected_region_scope and "非试验区域" not in selected_region_scope:
        df = df[df["试验分组"].isin(trial_groups)]
    elif "非试验区域" in selected_region_scope and "试验区域" not in selected_region_scope:
        df = df[df["试验分组"] == "非试验区域"]

    sku_meta = _build_sku_meta(data)
    if not sku_meta.empty:
        sku_meta = sku_meta.rename(columns={"商品名称": "商品名称_配置"})
        df = df.merge(sku_meta, on="sku_key", how="left")
        if "商品名称" in df.columns:
            df["商品名称"] = df["商品名称"].fillna(df.get("商品名称_配置"))
        else:
            df["商品名称"] = df.get("商品名称_配置")
    else:
        if "商品名称" not in df.columns:
            df["商品名称"] = pd.NA

    # 周期定义：
    # 1) 摸底期日期 -> "摸底期"
    # 2) 摸底期前日期 -> 以(摸底期起始日-1)为前1周结束日，每7天倒推为前N周
    if start_date is not None and end_date is not None:
        pre_anchor = start_date - pd.Timedelta(days=1)
        delta_pre = (pre_anchor - df["日期"]).dt.days
        pre_week_num = np.floor_divide(delta_pre, 7) + 1
        df["试验周期"] = np.where(
            (df["日期"] >= start_date) & (df["日期"] <= end_date),
            "摸底期",
            np.where(
                df["日期"] < start_date,
                pd.Series(pre_week_num, index=df.index).map(
                    lambda x: f"摸底期前{int(x)}周" if pd.notna(x) and x >= 1 else "摸底期前1周"
                ),
                "摸底期后",
            ),
        )
    else:
        df["试验周期"] = "未分配"

    # 仅保留：摸底期 + 摸底期前1~5周
    valid_cycles = ["摸底期"] + [f"摸底期前{i}周" for i in range(1, 6)]
    df = df[df["试验周期"].isin(valid_cycles)]

    if granularity == "日期":
        group_cols = ["日期", "市名称", "试验分组", "sku_key"]
    else:
        group_cols = ["试验周期", "市名称", "试验分组", "sku_key"]

    out = (
        df.groupby(group_cols, dropna=False)
        .agg(
            {
                "下单数量": "sum",
                "订单id": pd.Series.nunique,
                "店铺id": pd.Series.nunique,
                "送达金额": "sum",
                "送达抽佣金额": "sum",
            }
        )
        .reset_index()
    )

    df_name = _build_latest_product_name_map(df, group_cols)
    if not df_name.empty and "商品名称" in df_name.columns:
        name_cols = [c for c in group_cols + ["商品名称"] if c in df_name.columns]
        if name_cols:
            df_name = df_name[name_cols].drop_duplicates(subset=group_cols, keep="last")
            out = out.merge(df_name, on=group_cols, how="left")
    out = out.rename(
        columns={
            "市名称": "城市名称",
            "sku_key": "商品ID",
            "下单数量": "下单数量",
            "订单id": "订单数",
            "店铺id": "下单门店数",
            "送达金额": "货值",
            "送达抽佣金额": "抽佣金额",
        }
    )
    out["实际抽佣率"] = np.where(out["货值"] > 0, out["抽佣金额"] / out["货值"], np.nan)

    if granularity == "日期":
        out["日期"] = pd.to_datetime(out["日期"], errors="coerce").dt.date
        show_cols = [
            "日期",
            "城市名称",
            "试验分组",
            "商品ID",
            "商品名称",
            "下单数量",
            "订单数",
            "下单门店数",
            "货值",
            "抽佣金额",
            "实际抽佣率",
        ]
        # 表格可读性优先：最新日期在上
        out = out.sort_values(["日期", "城市名称", "商品ID"], ascending=[False, True, True])
    else:
        show_cols = [
            "试验周期",
            "城市名称",
            "试验分组",
            "商品ID",
            "商品名称",
            "下单数量",
            "订单数",
            "下单门店数",
            "货值",
            "抽佣金额",
            "实际抽佣率",
        ]
        # 表格可读性优先：摸底期 -> 前1周 -> ... -> 前5周
        out["_周期排序"] = out["试验周期"].map(_cycle_recent_rank)
        out = out.sort_values(["_周期排序", "城市名称", "商品ID"], ascending=[True, True, True])

    for c in ["下单数量", "订单数", "下单门店数"]:
        out[c] = out[c].fillna(0).astype(int)

    out["货值"] = out["货值"].map(lambda x: 0.0 if pd.isna(x) else float(x))
    out["抽佣金额"] = out["抽佣金额"].map(lambda x: 0.0 if pd.isna(x) else float(x))

    return out[[c for c in show_cols if c in out.columns]]


def _render_year_comparison(data: dict[str, pd.DataFrame]):
    st.subheader("同期对比")
    st.caption("数据范围：4月1日-7月31日")

    fact = data.get("fact_order_item", pd.DataFrame())
    if fact.empty or "日期" not in fact.columns:
        st.info("缺少事实数据，无法计算同期对比")
        return

    comp = fact.copy()
    comp.columns = [str(c).strip() for c in comp.columns]
    comp = _season_range_filter(comp, "日期")
    if comp.empty:
        st.info("当前数据在 4月1日-7月31日 范围内为空")
        return
    comp["日期"] = pd.to_datetime(comp["日期"], errors="coerce")
    comp = comp.dropna(subset=["日期"])
    comp["year"] = comp["日期"].dt.year

    year_options = sorted(comp["year"].dropna().astype(int).unique().tolist())
    default_years = year_options

    comp = _build_city_display(comp)
    city_options = [c for c in sorted(comp["city_pair"].dropna().unique().tolist()) if c]

    # 同期对比筛选器（3列布局）
    r1c1, r1c2, r1c3 = st.columns(3)
    with r1c1:
        selected_years = st.multiselect(
            "年份筛选（同比）",
            options=year_options,
            default=default_years,
            key="tab5_compare_years",
        )
    with r1c2:
        selected_city = st.selectbox(
            "城市筛选（同比）",
            options=["整体"] + city_options,
            index=0,
            key="tab5_compare_city",
        )
    with r1c3:
        compare_metric_label = st.selectbox(
            "指标筛选（同比）",
            options=["下单数量", "货值", "抽佣金额", "实际抽佣率", "下单门店数"],
            index=0,
            key="tab5_compare_metric",
        )

    if not selected_years:
        st.info("请至少选择一个年份")
        return
    comp = comp[comp["year"].isin(selected_years)]

    if selected_city != "整体":
        comp = comp[comp["city_pair"] == selected_city]

    # 属性单项筛选（三列）
    r2c1, r2c2, r2c3 = st.columns(3)
    with r2c1:
        grade_opts = sorted(comp["商品等级"].dropna().astype(str).unique().tolist()) if "商品等级" in comp.columns else []
        selected_grade = st.multiselect("商品等级", options=grade_opts, default=grade_opts, key="tab5_attr_grade")
    with r2c2:
        origin_opts = sorted(comp["产地"].dropna().astype(str).unique().tolist()) if "产地" in comp.columns else []
        selected_origin = st.multiselect("产地", options=origin_opts, default=origin_opts, key="tab5_attr_origin")
    with r2c3:
        pack_opts = sorted(comp["包装类型"].dropna().astype(str).unique().tolist()) if "包装类型" in comp.columns else []
        selected_pack = st.multiselect("包装类型", options=pack_opts, default=pack_opts, key="tab5_attr_pack")

    if selected_grade and "商品等级" in comp.columns:
        comp = comp[comp["商品等级"].astype(str).isin(selected_grade)]
    if selected_origin and "产地" in comp.columns:
        comp = comp[comp["产地"].astype(str).isin(selected_origin)]
    if selected_pack and "包装类型" in comp.columns:
        comp = comp[comp["包装类型"].astype(str).isin(selected_pack)]

    if comp.empty:
        st.info("筛选后无同比数据")
        return

    size_opts = sorted(comp["单果大小"].dropna().astype(str).unique().tolist()) if "单果大小" in comp.columns else []
    selected_size = st.multiselect("单果大小", options=size_opts, default=size_opts, key="tab5_attr_size")
    if selected_size and "单果大小" in comp.columns:
        comp = comp[comp["单果大小"].astype(str).isin(selected_size)]

    if comp.empty:
        st.info("筛选后无同比数据")
        return

    if compare_metric_label == "下单数量":
        metric_col = "下单数量"
    elif compare_metric_label == "货值":
        metric_col = "送达金额"
    elif compare_metric_label == "抽佣金额":
        metric_col = "送达抽佣金额"
    elif compare_metric_label == "下单门店数":
        metric_col = "店铺id"
    elif compare_metric_label == "实际抽佣率":
        metric_col = "实际抽佣率"
    else:
        metric_col = "送达金额"

    dim_cols = [c for c in ["商品等级", "产地", "包装类型", "单果大小"] if c in comp.columns]
    comp = _build_attr_combo(comp, dim_cols, combo_col="属性组合")

    # 先展示同比表格（不受属性组合/周期筛选影响）
    table_group_cols = ["year", "属性组合"]

    if compare_metric_label == "实际抽佣率":
        if not {"送达抽佣金额", "送达金额"}.issubset(comp.columns):
            st.info("缺少抽佣金额或货值字段，无法计算实际抽佣率")
            return
        comp["送达抽佣金额"] = pd.to_numeric(comp["送达抽佣金额"], errors="coerce")
        comp["送达金额"] = pd.to_numeric(comp["送达金额"], errors="coerce")
        agg_table = comp.groupby(table_group_cols, dropna=False)[["送达抽佣金额", "送达金额"]].sum().reset_index()
        agg_table[metric_col] = np.where(agg_table["送达金额"] > 0, agg_table["送达抽佣金额"] / agg_table["送达金额"], np.nan)
    elif compare_metric_label == "下单门店数":
        if "店铺id" not in comp.columns:
            st.info("缺少店铺id字段，无法计算下单门店数")
            return
        agg_table = comp.groupby(table_group_cols, dropna=False)["店铺id"].nunique().reset_index(name=metric_col)
    else:
        if metric_col not in comp.columns:
            st.info(f"缺少指标字段：{metric_col}")
            return
        comp[metric_col] = pd.to_numeric(comp[metric_col], errors="coerce")
        agg_table = comp.groupby(table_group_cols, dropna=False)[metric_col].sum().reset_index()

    # 同比明细表：按属性组合展示各年总值
    pivot = agg_table.pivot_table(index="属性组合", columns="year", values=metric_col, aggfunc="sum").reset_index()
    rename_map = {
        c: f"value_{int(c)}"
        for c in pivot.columns
        if isinstance(c, (int, np.integer))
    }
    pivot = pivot.rename(columns=rename_map)
    for y in selected_years:
        col = f"value_{y}"
        if col not in pivot.columns:
            pivot[col] = np.nan

    year_set = set(selected_years or [])
    if 2025 in year_set and 2026 in year_set:
        base_year, last_year = 2025, 2026
    elif selected_years and len(selected_years) >= 2:
        sorted_years = sorted(selected_years)
        base_year, last_year = sorted_years[-2], sorted_years[-1]
    else:
        base_year = None
        last_year = None
    if base_year is not None and last_year is not None and base_year != last_year:
        bcol = f"value_{base_year}"
        lcol = f"value_{last_year}"
        pivot["同比增幅"] = np.where(
            pivot[bcol] > 0,
            pivot[lcol] / pivot[bcol] - 1,
            np.nan,
        )
    else:
        pivot["同比增幅"] = np.nan

    show = pivot.copy()
    for y in selected_years:
        col = f"value_{y}"
        show[str(y)] = show[col].map(lambda x: _format_metric_value(compare_metric_label, x))
    growth_col = (
        f"同比增幅（{last_year} vs {base_year}）"
        if base_year is not None and last_year is not None and base_year != last_year
        else "同比增幅"
    )
    show[growth_col] = show["同比增幅"].map(lambda x: "-" if pd.isna(x) else f"{x:.1%}")
    show_cols = ["属性组合"] + [str(y) for y in selected_years] + [growth_col]
    st.dataframe(show[[c for c in show_cols if c in show.columns]], use_container_width=True)

    # 以下两个筛选器仅影响趋势图
    trend_c1, trend_c2 = st.columns(2)
    combo_opts = sorted(comp["属性组合"].dropna().astype(str).unique().tolist())
    with trend_c1:
        selected_combo = st.selectbox(
            "属性组合筛选（趋势图）",
            options=["全部"] + combo_opts,
            index=0,
            key="tab5_attr_combo_single",
        )
    with trend_c2:
        period_mode = st.radio("周期筛选（趋势图）", options=["日", "自然周"], horizontal=True, key="tab5_compare_period")

    trend_base = comp.copy()
    if selected_combo != "全部":
        trend_base = trend_base[trend_base["属性组合"] == selected_combo]

    if trend_base.empty:
        st.info("当前趋势图筛选无数据")
        return

    if period_mode == "自然周":
        iso = trend_base["日期"].dt.isocalendar()
        trend_base["时间轴"] = iso.week.astype(int).map(lambda w: f"第{int(w)}周")
        trend_base["时间排序"] = iso.week.astype(int)
        st.caption("周口径：自然周（ISO，周一至周日）")
    else:
        trend_base["时间轴"] = trend_base["日期"].dt.strftime("%m-%d")
        trend_base["时间排序"] = trend_base["日期"].dt.month * 100 + trend_base["日期"].dt.day

    # 趋势图：X=日/自然周，颜色=年份（legend）
    trend_group_cols = ["year", "时间轴", "时间排序"]
    if compare_metric_label == "实际抽佣率":
        chart_df = trend_base.groupby(trend_group_cols, dropna=False)[["送达抽佣金额", "送达金额"]].sum().reset_index()
        chart_df[metric_col] = np.where(chart_df["送达金额"] > 0, chart_df["送达抽佣金额"] / chart_df["送达金额"], np.nan)
    elif compare_metric_label == "下单门店数":
        chart_df = trend_base.groupby(trend_group_cols, dropna=False)["店铺id"].nunique().reset_index(name=metric_col)
    else:
        chart_df = trend_base.groupby(trend_group_cols, dropna=False)[metric_col].sum().reset_index()

    # 日粒度按固定 4/1~7/30 月-日轴对齐，确保 x 轴仅月-日、legend 区分年份
    if period_mode == "日" and not chart_df.empty:
        full_day_axis = pd.date_range("2001-04-01", "2001-07-31", freq="D").strftime("%m-%d").tolist()
        axis_df = pd.DataFrame({"时间轴": full_day_axis})
        axis_df["时间排序"] = axis_df["时间轴"].str.replace("-", "").astype(int)

        completed = []
        for y in sorted(chart_df["year"].dropna().astype(int).unique().tolist()):
            one = chart_df[chart_df["year"] == y].copy()
            one = axis_df.merge(one, on=["时间轴", "时间排序"], how="left")
            one["year"] = y
            if compare_metric_label == "实际抽佣率":
                # 比率无分母时保持缺失，避免补0误导趋势
                one[metric_col] = one[metric_col]
            else:
                one[metric_col] = one[metric_col].fillna(0)
            completed.append(one)
        if completed:
            chart_df = pd.concat(completed, ignore_index=True)

    chart_df = chart_df.sort_values(["时间排序", "year"])
    chart_df["时间轴"] = chart_df["时间轴"].astype(str)
    chart_df["year_str"] = chart_df["year"].astype(str)
    fig = px.line(
        chart_df,
        x="时间轴",
        y=metric_col,
        color="year_str",
        markers=True,
        title=f"同期趋势（{period_mode}）",
    )
    if compare_metric_label == "实际抽佣率":
        fig.update_yaxes(tickformat=".2%")
    if period_mode == "日":
        fig.update_xaxes(title_text="月-日", type="category")
    else:
        fig.update_xaxes(title_text="自然周（ISO）", type="category")

    ordered_axis = (
        chart_df[["时间轴", "时间排序"]]
        .drop_duplicates()
        .sort_values("时间排序")["时间轴"]
        .tolist()
    )
    fig.update_xaxes(categoryorder="array", categoryarray=ordered_axis)
    if period_mode == "日":
        sparse_ticks = ordered_axis[::7] if len(ordered_axis) > 7 else ordered_axis
        fig.update_xaxes(tickmode="array", tickvals=sparse_ticks, ticktext=sparse_ticks)
    st.plotly_chart(fig, use_container_width=True)


def render(data: dict[str, pd.DataFrame]):
    wide = data.get("agg_wide_table", pd.DataFrame())

    if wide.empty:
        st.info("暂无聚合宽表数据")
        return

    wide = wide.copy()
    wide["sku_key"] = wide.get("sku_id", pd.Series([pd.NA] * len(wide))).map(_canonical_sku)

    available_metric_labels = [
        label for label, col in METRIC_LABEL_TO_COL.items() if col in wide.columns
    ]
    if not available_metric_labels:
        st.info("暂无可展示指标")
        return

    sku_meta = _build_sku_meta(data)
    trial_sku_set = _build_trial_sku_set(data)
    if trial_sku_set:
        sku_meta = sku_meta[sku_meta["sku_key"].isin(trial_sku_set)]

    city_candidates = set()
    fact = data.get("fact_order_item", pd.DataFrame())
    if not fact.empty and "市名称" in fact.columns:
        city_candidates |= {str(c) for c in fact["市名称"].dropna().astype(str).tolist() if str(c).strip()}
    if "city_unit" in wide.columns:
        city_candidates |= {str(c) for c in wide["city_unit"].dropna().astype(str).tolist() if str(c).strip()}
    city_options = sorted(city_candidates)

    tg_map_df = _build_trial_group_map(data.get("conf_trial_group", pd.DataFrame()))
    city_group_map = {}
    if not tg_map_df.empty:
        for _, r in tg_map_df.iterrows():
            city = str(r.get("市名称", "")).strip()
            if not city:
                continue
            grp = str(r.get("试验分组", "")).strip() or "非试验区"
            city_group_map[city] = grp

    city_label_map = {
        c: f"{c}-{city_group_map.get(c, '非试验区')}"
        for c in city_options
    }

    c1, c2, c3 = st.columns(3)
    with c1:
        selected_metric_label = st.selectbox(
            "指标筛选",
            options=available_metric_labels,
            index=0,
            key="tab5_metric_selector",
        )

    selected_skus = []
    with c2:
        if not sku_meta.empty:
            sku_options = sku_meta["sku_key"].tolist()
            sku_name_map = dict(zip(sku_meta["sku_key"], sku_meta["商品名称"]))
            selected_skus = st.multiselect(
                "商品筛选",
                options=sku_options,
                default=sku_options,
                key="tab5_sku_selector",
                format_func=lambda s: _sku_label(s, sku_name_map),
            )
            if selected_skus:
                wide = wide[wide["sku_key"].isin(selected_skus)]
        else:
            st.info("暂无可筛选的试验商品")

    selected_cities = []
    with c3:
        selected_region_scope = st.multiselect(
            "区域类型筛选",
            options=["试验区域", "非试验区域"],
            default=["试验区域", "非试验区域"],
            key="tab5_baseline_region_scope",
        )

        trial_groups = {"对照组", "试验组一", "试验组二", "试验组三", "实验组一", "实验组二", "实验组三"}

        def _is_trial_city(city_name: str) -> bool:
            grp = city_group_map.get(city_name, "非试验区")
            return grp in trial_groups

        filtered_city_options = city_options
        if selected_region_scope:
            if "试验区域" in selected_region_scope and "非试验区域" not in selected_region_scope:
                filtered_city_options = [c for c in city_options if _is_trial_city(c)]
            elif "非试验区域" in selected_region_scope and "试验区域" not in selected_region_scope:
                filtered_city_options = [c for c in city_options if not _is_trial_city(c)]

        if filtered_city_options:
            selected_cities = st.multiselect(
                "城市筛选",
                options=filtered_city_options,
                default=filtered_city_options,
                key="tab5_city_selector",
                format_func=lambda c: city_label_map.get(c, f"{c}-非试验区"),
            )
        else:
            st.info("当前区域类型下无可选城市")

    if selected_cities and "city_unit" in wide.columns:
        wide = wide[wide["city_unit"].isin(selected_cities)]

    effect = wide[wide["stage"] == "生效期"].copy()
    baseline = wide[wide["stage"] == "摸底期"].copy()

    if effect.empty:
        st.subheader("摸底期实际监控（不触发预警）")
        granularity = st.radio(
            "监控粒度",
            options=["试验周期", "日期"],
            horizontal=True,
            key="tab5_baseline_granularity",
        )

        monitor_df = _build_baseline_monitor_df(
            data,
            selected_skus,
            selected_cities,
            selected_region_scope,
            granularity,
        )
        if monitor_df.empty:
            st.info("暂无摸底期监控数据")
        else:
            st.caption("当前阶段为摸底期，展示实际指标，不计算 WoW 告警。")
            st.dataframe(monitor_df, use_container_width=True)

            # 摸底期图表
            metric_col = METRIC_LABEL_TO_COL[selected_metric_label]
            if selected_metric_label in {"下单数量", "货值", "抽佣金额", "下单门店数", "实际抽佣率"}:
                chart_df = monitor_df.copy()
                if selected_metric_label == "下单数量":
                    y_col = "下单数量"
                elif selected_metric_label == "货值":
                    y_col = "货值"
                elif selected_metric_label == "抽佣金额":
                    y_col = "抽佣金额"
                elif selected_metric_label == "下单门店数":
                    y_col = "下单门店数"
                else:
                    y_col = "实际抽佣率"

                if selected_metric_label == "下单数量" and "日期" in chart_df.columns:
                    grouped = chart_df.groupby("日期", dropna=False)[y_col].sum().reset_index().sort_values("日期")
                    grouped["日期"] = pd.to_datetime(grouped["日期"], errors="coerce")
                    fig = px.line(grouped, x="日期", y=y_col, markers=True, title="下单数量趋势（按日期）")
                    fig.update_xaxes(tickformat="%Y-%m-%d")
                else:
                    if granularity == "日期" and "日期" in chart_df.columns:
                        grouped = chart_df.groupby("日期", dropna=False)[y_col].sum().reset_index()
                        grouped["日期"] = pd.to_datetime(grouped["日期"], errors="coerce")
                        grouped = grouped.sort_values("日期")
                        x_col = "日期"
                    else:
                        grouped = chart_df.groupby("试验周期", dropna=False)[y_col].sum().reset_index()
                        grouped["_周期排序"] = grouped["试验周期"].map(_cycle_timeline_rank)
                        grouped = grouped.sort_values("_周期排序")
                        x_col = "试验周期"
                    fig = px.bar(grouped, x=x_col, y=y_col, title=f"摸底期{selected_metric_label}趋势")
                    if x_col == "日期":
                        fig.update_xaxes(tickformat="%Y-%m-%d")
                    if x_col == "试验周期":
                        cycle_order = [f"摸底期前{i}周" for i in range(5, 0, -1)] + ["摸底期"]
                        fig.update_xaxes(categoryorder="array", categoryarray=cycle_order)
                    fig = _apply_bar_value_labels(fig)
                if selected_metric_label == "实际抽佣率":
                    fig.update_yaxes(tickformat=".2%")
                st.plotly_chart(fig, use_container_width=True)

                # 试验区域城市分布图（过滤非试验区域）
                city_df = chart_df[chart_df.get("试验分组", pd.Series([""] * len(chart_df))).astype(str) != "非试验区域"].copy()
                if not city_df.empty and "城市名称" in city_df.columns:
                    if selected_metric_label == "实际抽佣率" and {"抽佣金额", "货值"}.issubset(city_df.columns):
                        if granularity == "日期" and "日期" in city_df.columns:
                            city_grouped = (
                                city_df.groupby(["日期", "城市名称"], dropna=False)[["抽佣金额", "货值"]]
                                .sum()
                                .reset_index()
                            )
                            city_grouped["日期"] = pd.to_datetime(city_grouped["日期"], errors="coerce")
                            city_grouped = city_grouped.sort_values("日期")
                            x2_col = "日期"
                        else:
                            city_grouped = (
                                city_df.groupby(["试验周期", "城市名称"], dropna=False)[["抽佣金额", "货值"]]
                                .sum()
                                .reset_index()
                            )
                            city_grouped["_周期排序"] = city_grouped["试验周期"].map(_cycle_timeline_rank)
                            city_grouped = city_grouped.sort_values("_周期排序")
                            x2_col = "试验周期"
                        city_grouped[y_col] = np.where(city_grouped["货值"] > 0, city_grouped["抽佣金额"] / city_grouped["货值"], np.nan)
                    else:
                        if granularity == "日期" and "日期" in city_df.columns:
                            city_grouped = city_df.groupby(["日期", "城市名称"], dropna=False)[y_col].sum().reset_index()
                            city_grouped["日期"] = pd.to_datetime(city_grouped["日期"], errors="coerce")
                            city_grouped = city_grouped.sort_values("日期")
                            x2_col = "日期"
                        else:
                            city_grouped = city_df.groupby(["试验周期", "城市名称"], dropna=False)[y_col].sum().reset_index()
                            city_grouped["_周期排序"] = city_grouped["试验周期"].map(_cycle_timeline_rank)
                            city_grouped = city_grouped.sort_values("_周期排序")
                            x2_col = "试验周期"

                    fig_city = px.bar(
                        city_grouped,
                        x=x2_col,
                        y=y_col,
                        color="城市名称",
                        barmode="group",
                        title=f"摸底期{selected_metric_label}趋势（试验区域城市）",
                    )
                    if selected_metric_label == "实际抽佣率":
                        fig_city.update_yaxes(tickformat=".2%")
                    if x2_col == "日期":
                        fig_city.update_xaxes(tickformat="%Y-%m-%d")
                    if x2_col == "试验周期":
                        cycle_order = [f"摸底期前{i}周" for i in range(5, 0, -1)] + ["摸底期"]
                        fig_city.update_xaxes(categoryorder="array", categoryarray=cycle_order)
                    fig_city = _apply_bar_value_labels(fig_city)
                    st.plotly_chart(fig_city, use_container_width=True)

        _render_year_comparison(data)
        return

    st.subheader("城市 x SKU 告警状态表")

    if "stage_week" not in effect.columns:
        st.info("生效期数据缺少 stage_week 列")
        return

    effect = effect.sort_values(["city_unit", "sku_key", "stage_week"])

    alert_rows = []
    thresholds = ALERT_THRESHOLDS.get("生效期", {})
    order_yellow = thresholds.get("order_count_wow_yellow", -0.10)
    order_red = thresholds.get("order_count_wow_red", -0.15)
    store_yellow = thresholds.get("active_store_count_wow_yellow", -0.05)
    store_red = thresholds.get("active_store_count_wow_red", -0.10)

    for (city, sku), group in effect.groupby(["city_unit", "sku_key"]):
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

                if not np.isnan(order_wow) and order_wow < order_red:
                    alert = "RED"
                elif not np.isnan(order_wow) and order_wow < order_yellow:
                    alert = "YELLOW"

                if not np.isnan(store_wow) and store_wow < store_red:
                    alert = "RED"
                elif alert != "RED" and not np.isnan(store_wow) and store_wow < store_yellow:
                    alert = "YELLOW"

            alert_rows.append(
                {
                    "试验周期": week,
                    "城市名称": city,
                    "商品ID": sku,
                    "周完整": is_complete,
                    "订单数": row_data.get("order_count", np.nan),
                    "下单门店数": row_data.get("active_store_count", np.nan),
                    "订单数环比": order_wow,
                    "门店数环比": store_wow,
                    "告警等级": alert,
                }
            )

    if alert_rows:
        alert_df = pd.DataFrame(alert_rows)
        display_df = alert_df.copy()
        display_df["告警"] = display_df["告警等级"].apply(lambda x: render_alert_badge(x) if x != "N/A" else "-")
        for col in ["订单数环比", "门店数环比"]:
            display_df[col] = display_df[col].map(lambda v: f"{v:.1%}" if pd.notna(v) else "-")

        st.dataframe(
            display_df[["告警", "试验周期", "城市名称", "商品ID", "订单数", "下单门店数", "订单数环比", "门店数环比"]],
            use_container_width=True,
        )

        metric_col = METRIC_LABEL_TO_COL[selected_metric_label]
        if metric_col in effect.columns and "stage_week" in effect.columns:
            if selected_metric_label == "实际抽佣率" and {"commission_amount", "gmv"}.issubset(effect.columns):
                chart_df = (
                    effect.groupby("stage_week", dropna=False)[["commission_amount", "gmv"]]
                    .sum()
                    .reset_index()
                    .sort_values("stage_week")
                )
                chart_df[metric_col] = np.where(chart_df["gmv"] > 0, chart_df["commission_amount"] / chart_df["gmv"], np.nan)
            else:
                chart_df = (
                    effect.groupby("stage_week", dropna=False)[metric_col]
                    .sum()
                    .reset_index()
                    .sort_values("stage_week")
                )

            fig_metric = px.line(
                chart_df,
                x="stage_week",
                y=metric_col,
                markers=True,
                title=f"生效期{selected_metric_label}趋势",
            )
            if selected_metric_label == "实际抽佣率":
                fig_metric.update_yaxes(tickformat=".2%")
            st.plotly_chart(fig_metric, use_container_width=True)

        red_count = len(alert_df[alert_df["告警等级"] == "RED"])
        yellow_count = len(alert_df[alert_df["告警等级"] == "YELLOW"])
        green_count = len(alert_df[alert_df["告警等级"] == "GREEN"])
        cols = st.columns(3)
        cols[0].metric("正常", green_count)
        cols[1].metric("预警", yellow_count)
        cols[2].metric("告警", red_count)
    else:
        st.info("无法计算环比数据")

    st.subheader("缺货数量趋势")
    if "stockout_num" in effect.columns:
        stockout = effect.groupby("stage_week")["stockout_num"].sum().reset_index()
        if not stockout.empty:
            fig = px.line(stockout, x="stage_week", y="stockout_num", title="生效期缺货数量趋势")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("暂无缺货数据")

    st.subheader("告警阈值参考")
    threshold_rows = []
    for stage, metrics in ALERT_THRESHOLDS.items():
        for metric_name, value in metrics.items():
            threshold_rows.append({"阶段": stage, "指标": metric_name, "阈值": f"{value:.0%}"})
    st.dataframe(pd.DataFrame(threshold_rows), use_container_width=True)

    _render_year_comparison(data)
