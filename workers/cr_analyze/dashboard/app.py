# coding:utf8
"""workers.cr_analyze.dashboard.app -- Streamlit 看板入口

启动方式:
    streamlit run workers/cr_analyze/dashboard/app.py
    streamlit run workers/cr_analyze/dashboard/app.py -- --db-path /path/to/db
"""

import sys

import streamlit as st

from workers.cr_analyze.config import DEFAULT_DB_PATH
from .components import load_db, check_db, render_error_missing_db
from . import (
    tab1_overview,
    tab2_config_audit,
    tab3_normalization,
    tab4_effect,
    tab5_guardrail,
    tab_power,
)

st.set_page_config(
    page_title="抽佣率试验分析看板",
    page_icon="📊",
    layout="wide",
)


def _parse_db_path() -> str:
    """从 Streamlit 参数中解析 db-path。"""
    try:
        idx = sys.argv.index("--db-path")
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    except (ValueError, IndexError):
        pass
    return str(DEFAULT_DB_PATH)


def main():
    db_path = _parse_db_path()

    st.title("📊 抽佣率试验分析看板")
    st.caption(f"数据源: `{db_path}`")

    if not check_db(db_path):
        render_error_missing_db()
        return

    data = load_db(db_path)
    if not data:
        render_error_missing_db()
        return

    tab_labels = [
        "试验总览",
        "配置核查",
        "归一化进度",
        "效应分析",
        "护栏预警",
        "功效分析",
    ]
    tab_modules = [
        tab1_overview,
        tab2_config_audit,
        tab3_normalization,
        tab4_effect,
        tab5_guardrail,
        tab_power,
    ]

    tabs = st.tabs(tab_labels)
    for tab_widget, module in zip(tabs, tab_modules):
        with tab_widget:
            try:
                module.render(data)
            except Exception as e:
                st.error(f"渲染出错: {e}")


if __name__ == "__main__":
    main()
