# coding:utf8
"""Tab 1: 试验总览"""

from datetime import date

import pandas as pd
import streamlit as st

from workers.cr_analyze.config import TRIAL_PHASE_CONFIG


def _to_date_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df


def _build_phase_status_table(phase_wide: pd.DataFrame) -> pd.DataFrame:
    if phase_wide.empty:
        return pd.DataFrame()

    phase = phase_wide.copy()
    phase = _to_date_col(phase, "试验起始日期")
    phase = _to_date_col(phase, "试验结束日期")

    keep = [c for c in ["试验阶段", "试验起始日期", "试验结束日期"] if c in phase.columns]
    if len(keep) < 3:
        return pd.DataFrame()

    phase_tbl = phase[keep].dropna(subset=["试验阶段", "试验起始日期", "试验结束日期"])
    phase_tbl = phase_tbl.drop_duplicates().sort_values(["试验起始日期", "试验结束日期"])

    today = date.today()

    def _flag(row):
        if row["试验起始日期"] <= today <= row["试验结束日期"]:
            return "🟢 进行中"
        return ""

    phase_tbl["当前阶段"] = phase_tbl.apply(_flag, axis=1)
    return phase_tbl.reset_index(drop=True)


def _build_phase_pivot(phase_wide: pd.DataFrame, phase_pivot: pd.DataFrame) -> pd.DataFrame:
    group_order = ["对照组", "试验组一", "试验组二", "试验组三"]

    if phase_pivot is not None and not phase_pivot.empty:
        out = phase_pivot.copy()
        out = _to_date_col(out, "试验起始日期")
        out = _to_date_col(out, "试验结束日期")
        if "试验分组" in out.columns:
            out["试验分组"] = pd.Categorical(
                out["试验分组"],
                categories=group_order,
                ordered=True,
            )
            out = out.sort_values(["试验分组", "市名称", "试验阶段", "试验起始日期"]) \
                .reset_index(drop=True)
            out["试验分组"] = out["试验分组"].astype(str)
        return out

    if phase_wide is None or phase_wide.empty:
        return pd.DataFrame()

    base_cols = ["市名称", "试验分组", "试验阶段", "试验起始日期", "试验结束日期", "运营类型", "抽佣率"]
    if any(c not in phase_wide.columns for c in base_cols):
        return pd.DataFrame()

    base = phase_wide[base_cols].copy()
    base = _to_date_col(base, "试验起始日期")
    base = _to_date_col(base, "试验结束日期")

    pivot = base.pivot_table(
        index=["市名称", "试验分组", "试验阶段", "试验起始日期", "试验结束日期"],
        columns="运营类型",
        values="抽佣率",
        aggfunc="first",
    ).reset_index()
    pivot.columns.name = None
    pivot = pivot.dropna(subset=["市名称", "试验分组"], how="any")
    pivot["试验分组"] = pd.Categorical(
        pivot["试验分组"],
        categories=group_order,
        ordered=True,
    )
    pivot = pivot.sort_values(["试验分组", "市名称", "试验阶段", "试验起始日期"]).reset_index(drop=True)
    pivot["试验分组"] = pivot["试验分组"].astype(str)
    return pivot


def _build_holiday_notice(phase_status: pd.DataFrame) -> str:
    dragon_boat = TRIAL_PHASE_CONFIG.get("dragon_boat_dates", [])
    if not dragon_boat:
        return "未配置端午特殊时段。"

    holiday_start = min(dragon_boat)
    holiday_end = max(dragon_boat)
    extension_days = int(TRIAL_PHASE_CONFIG.get("holiday_extension_days", 3))
    baseline_min_days = int(TRIAL_PHASE_CONFIG.get("baseline_min_effective_days", 7))

    today = date.today()
    decision = "无需延长3天"

    if "当前阶段" in phase_status.columns:
        current_rows = phase_status[phase_status["当前阶段"] == "🟢 进行中"]
    else:
        current_rows = pd.DataFrame()
    if not current_rows.empty:
        row = current_rows.iloc[0]
        stage_start = row.get("试验起始日期")
        stage_end = row.get("试验结束日期")

        if pd.notna(stage_start) and pd.notna(stage_end):
            effective_end = min(today, stage_end)
            if stage_start <= effective_end:
                all_days = pd.date_range(stage_start, effective_end, freq="D").date
                trading_days = [d for d in all_days if d not in set(dragon_boat)]
                if len(trading_days) < baseline_min_days:
                    decision = f"建议延长{extension_days}天"

    return (
        f"端午特殊时段从 {holiday_start} 开始，至 {holiday_end} 结束。"
        f"当前评估结果：{decision}。"
    )


def render(data: dict[str, pd.DataFrame]):
    phase_wide = data.get("trial_phase_config_wide", pd.DataFrame())
    phase_pivot = data.get("trial_phase_config_pivot", pd.DataFrame())
    sku_profile = data.get("trial_sku_profile", pd.DataFrame())

    # 试验阶段表
    st.subheader("试验当前阶段")
    phase_status = _build_phase_status_table(phase_wide)
    if phase_status.empty:
        st.info("暂无试验阶段配置数据")
    else:
        st.dataframe(phase_status, use_container_width=True)

    # 城市分组配置透视
    st.subheader("城市分组配置")
    city_pivot = _build_phase_pivot(phase_wide, phase_pivot)
    if not city_pivot.empty:
        display_df = city_pivot.copy()
        display_df = display_df.drop(columns=["record_id"], errors="ignore")
        st.dataframe(display_df, use_container_width=True)
    else:
        st.info("暂无城市分组配置数据")

    # 关键时间节点
    st.subheader("关键时间节点")
    st.info(_build_holiday_notice(phase_status))

    # SKU 清单
    st.subheader("试验 SKU 清单")
    if sku_profile is None or sku_profile.empty:
        st.info("暂无试验 SKU 主数据，请先运行数据管道。")
    else:
        sku_df = sku_profile.copy()
        sku_df = _to_date_col(sku_df, "last_trial_date")
        show_cols = ["商品id", "商品名称", "商家名称", "非试验区域抽佣率", "last_trial_date"]
        show_cols = [c for c in show_cols if c in sku_df.columns]
        st.dataframe(sku_df[show_cols].drop_duplicates(), use_container_width=True)
