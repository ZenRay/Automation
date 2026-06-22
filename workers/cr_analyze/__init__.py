# coding:utf8
"""workers.cr_analyze -- 抽佣率试验分析模块

从飞书多维表格 + MaxCompute 提取数据，存储到 SQLite，
通过 Streamlit 看板进行交互式分析。
"""

from .main import run_cr_analyze_pipeline

__all__ = ["run_cr_analyze_pipeline"]
