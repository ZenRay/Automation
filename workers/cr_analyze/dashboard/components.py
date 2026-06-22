# coding:utf8
"""workers.cr_analyze.dashboard.components -- 共享 UI 组件"""

import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st

from workers.cr_analyze.config import DEFAULT_DB_PATH, ALERT_THRESHOLDS


@st.cache_data(show_spinner="加载数据...")
def load_db(db_path: str) -> dict[str, pd.DataFrame]:
    """从 SQLite 加载所有表到 dict。"""
    if not Path(db_path).exists():
        return {}

    tables: dict[str, pd.DataFrame] = {}
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        for (name,) in rows:
            tables[name] = pd.read_sql(f"SELECT * FROM [{name}]", conn)
    return tables


def check_db(db_path: str) -> bool:
    """检查 SQLite 是否存在且非空。"""
    p = Path(db_path)
    return p.exists() and p.stat().st_size > 0


def render_error_missing_db():
    """显示数据库缺失错误。"""
    st.error(
        "未找到 SQLite 数据库文件。请先运行数据管道：\n\n"
        "```bash\npython -m workers.cr_analyze.main\n```"
    )


def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """应用筛选器到 DataFrame。"""
    mask = pd.Series(True, index=df.index)

    if "date_range" in filters and "日期" in df.columns:
        start, end = filters["date_range"]
        df_dates = pd.to_datetime(df["日期"], errors="coerce").dt.date
        mask &= (df_dates >= start) & (df_dates <= end)

    if "sku_ids" in filters and filters["sku_ids"] and "sku_id" in df.columns:
        mask &= df["sku_id"].isin(filters["sku_ids"])

    if "city_units" in filters and filters["city_units"] and "city_unit" in df.columns:
        mask &= df["city_unit"].isin(filters["city_units"])

    return df[mask]


def render_metric_card(label: str, value, delta=None, delta_color="normal"):
    """渲染指标卡片。"""
    st.metric(label=label, value=value, delta=delta, delta_color=delta_color)


def render_alert_badge(level: str) -> str:
    """返回告警等级标记。"""
    badges = {
        "GREEN": "🟢",
        "YELLOW": "🟡",
        "RED": "🔴",
    }
    return badges.get(level, "⚪")
