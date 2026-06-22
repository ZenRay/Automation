# coding:utf8
"""tests/cr_analyze/test_dashboard_e2e.py -- 看板冒烟 E2E 测试

验证每个 tab 模块的 render 函数可正常调用（不抛异常）。
使用 mock 数据构造 SQLite，然后逐个调用 tab.render()。
"""

import sqlite3
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest


@pytest.fixture
def dashboard_db(tmp_path) -> str:
    """构造包含所有表的 SQLite 数据库。"""
    db_path = str(tmp_path / "dashboard_test.db")

    tables = {
        "conf_product_info": pd.DataFrame(
            {
                "日期": [date(2026, 6, 20)] * 2,
                "商品id": [10184690, 20519020],
                "商品名称": ["云南水仙芒大果", "云南水仙芒中果"],
                "产地": ["云南", "云南"],
                "包装类型": ["泡沫箱", "泡沫箱"],
                "单果大小": ["大果", "中果"],
                "色号": ["5号色", "5号色"],
                "商品头数": ["12", "15"],
                "非试验区域平台销售斤单价": [6.0, 5.5],
                "非试验区域平台销售件单价": [140.0, 120.0],
                "非试验区域商家供货斤单价": [5.4, 4.9],
                "非试验区域商家供货件单价": [126.0, 107.0],
                "是否当日上架": [0, 1],
            }
        ),
        "conf_county_info": pd.DataFrame(
            {
                "日期": [date(2026, 6, 20)],
                "区县id": [430201],
                "区县名称": ["荷塘区"],
                "市id": [430200],
                "市名称": ["株洲市"],
                "省id": [430000],
                "省名称": ["湖南省"],
                "运营类型": ["自营区域"],
            }
        ),
        "conf_commission_adjustment": pd.DataFrame(
            {
                "日期": [date(2026, 6, 20)],
                "商品id": [10184690],
                "区县名称": ["荷塘区"],
                "区域全称": ["湖南省-株洲市-荷塘区"],
                "调整系数": [1.0],
                "固定抽佣率调整": [0.02],
                "固定抽佣金额调整": [12.0],
                "参与试验类型": ["[试验区域]"],
            }
        ),
        "conf_trial_region_price": pd.DataFrame(
            {
                "日期": [date(2026, 6, 20)],
                "商品id": [10184690],
                "商品名称": ["云南水仙芒大果"],
                "商家名称": ["得兴果业"],
                "区域全称": ["湖南省-株洲市"],
                "试验区域平台销售斤单价": [6.5],
                "试验区域平台销售件单价": [150.0],
                "试验区域商家供货斤单价": [5.8],
                "试验区域商家供货件单价": [134.0],
                "抽佣率": [0.095],
            }
        ),
        "conf_trial_group": pd.DataFrame(
            {
                "区域id": [430200],
                "区域名称": ["株洲市"],
                "市名称": ["株洲市"],
                "区域类型": ["CITY"],
                "试验分组": ["试验组一"],
                "试验起始日期": [date(2026, 6, 19)],
                "试验结束日期": [date(2026, 7, 19)],
            }
        ),
        "conf_trial_period_rate": pd.DataFrame(
            {
                "试验阶段": ["摸底期", "生效期"],
                "运营类型": ["自营区域", "自营区域"],
                "抽佣率": [0.075, 0.095],
                "试验分组": ["试验组一", "试验组一"],
                "试验起始日期": [date(2026, 6, 19), date(2026, 6, 29)],
                "试验结束日期": [date(2026, 6, 28), date(2026, 7, 19)],
            }
        ),
        "agg_wide_table": pd.DataFrame(
            {
                "stage": ["归一化预备期", "摸底期", "生效期", "生效期"],
                "日期": [date(2026, 6, 19), None, None, None],
                "stage_week": [None, None, "生效期_W1", "生效期_W2"],
                "is_complete_week": [None, None, True, False],
                "trading_days": [1, 7, 7, 4],
                "city_unit": ["株洲市", "株洲市", "株洲市", "株洲市"],
                "region_type": [
                    "自营区域",
                    "自营区域",
                    "自营区域",
                    "自营区域",
                ],
                "trial_group": [
                    "试验组一",
                    "试验组一",
                    "试验组一",
                    "试验组一",
                ],
                "sku_id": [10184690, 10184690, 10184690, 10184690],
                "sku_origin": ["云南", "云南", "云南", "云南"],
                "sku_grade": ["5号色", "5号色", "5号色", "5号色"],
                "sku_weight_spec": ["泡沫箱", "泡沫箱", "泡沫箱", "泡沫箱"],
                "order_count": [10, 50, 30, 15],
                "active_store_count": [8, 20, 18, 12],
                "gmv": [1000.0, 5000.0, 3000.0, 1500.0],
                "commission_amount": [80.0, 400.0, 285.0, 142.5],
                "stockout_num": [0, 2, 1, 0],
                "commission_rate": [0.08, 0.08, 0.095, 0.095],
                "supply_price": [5.5, 5.5, 5.8, 5.8],
                "target_r0": [0.075, 0.075, 0.095, 0.095],
            }
        ),
        "power_analysis": pd.DataFrame(
            {
                "sku_id": [10184690, 20519020, 20588413],
                "sigma_raw": [0.194, 0.215, 0.188],
                "sigma_adjusted": [0.290, 0.322, 0.282],
                "rho_pre": [0.995, 0.980, 0.990],
                "rho_post": [0.991, 0.975, 0.985],
                "rho_main": [0.991, 0.975, 0.985],
                "n_required": [1.2, 1.8, 1.1],
                "n_actual": [2, 2, 2],
                "power_sufficient": [True, True, True],
                "fallback": [False, False, False],
                "n_weeks_available": [4, 4, 4],
            }
        ),
    }

    with sqlite3.connect(db_path) as conn:
        for name, df in tables.items():
            df.to_sql(name, conn, if_exists="replace", index=False)

    return db_path


class _MockContainer:
    """模拟 st.columns/st.tabs 返回的容器对象，支持任意方法调用。"""

    def __getattr__(self, name):
        return lambda *a, **kw: None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


@pytest.fixture
def mock_streamlit(monkeypatch):
    """完整 mock streamlit API，使 render() 可实际调用。"""
    import streamlit as st

    noop = lambda *a, **kw: None

    # cache_data: 返回装饰器
    def mock_cache_data(*a, **kw):
        def decorator(func):
            return func
        return decorator

    monkeypatch.setattr(st, "cache_data", mock_cache_data)

    # columns: 返回 N 个 mock 容器
    def mock_columns(n, *a, **kw):
        return [_MockContainer() for _ in range(n)]

    monkeypatch.setattr(st, "columns", mock_columns)

    # tabs: 返回 mock 上下文管理器列表
    def mock_tabs(labels, *a, **kw):
        return [_MockContainer() for _ in labels]

    monkeypatch.setattr(st, "tabs", mock_tabs)

    # radio / selectbox: 返回第一个选项
    def mock_select(label, options, *a, **kw):
        return options[0] if options else None

    monkeypatch.setattr(st, "radio", mock_select)
    monkeypatch.setattr(st, "selectbox", mock_select)

    # 所有其他函数: no-op
    for attr in [
        "title",
        "subheader",
        "caption",
        "write",
        "info",
        "warning",
        "error",
        "success",
        "metric",
        "dataframe",
        "line_chart",
        "bar_chart",
        "plotly_chart",
        "set_page_config",
        "markdown",
        "header",
        "divider",
        "spinner",
    ]:
        if hasattr(st, attr):
            monkeypatch.setattr(st, attr, noop)


@pytest.mark.integration
class TestDashboardSmoke:
    def test_tab1_overview_renders(self, dashboard_db, mock_streamlit):
        """Tab 1 试验总览 render() 实际调用不抛异常。"""
        from workers.cr_analyze.dashboard import tab1_overview

        data = _load_db_direct(dashboard_db)
        tab1_overview.render(data)

    def test_tab2_config_audit_renders(self, dashboard_db, mock_streamlit):
        """Tab 2 配置核查 render() 实际调用不抛异常。"""
        from workers.cr_analyze.dashboard import tab2_config_audit

        data = _load_db_direct(dashboard_db)
        tab2_config_audit.render(data)

    def test_tab3_normalization_renders(self, dashboard_db, mock_streamlit):
        """Tab 3 归一化进度 render() 实际调用不抛异常。"""
        from workers.cr_analyze.dashboard import tab3_normalization

        data = _load_db_direct(dashboard_db)
        tab3_normalization.render(data)

    def test_tab4_effect_renders(self, dashboard_db, mock_streamlit):
        """Tab 4 效应分析 render() 实际调用不抛异常。"""
        from workers.cr_analyze.dashboard import tab4_effect

        data = _load_db_direct(dashboard_db)
        tab4_effect.render(data)

    def test_tab5_guardrail_renders(self, dashboard_db, mock_streamlit):
        """Tab 5 护栏预警 render() 实际调用不抛异常。"""
        from workers.cr_analyze.dashboard import tab5_guardrail

        data = _load_db_direct(dashboard_db)
        tab5_guardrail.render(data)

    def test_tab_power_renders(self, dashboard_db, mock_streamlit):
        """Tab 6 功效分析 render() 实际调用不抛异常。"""
        from workers.cr_analyze.dashboard import tab_power

        data = _load_db_direct(dashboard_db)
        tab_power.render(data)

    def test_app_module_importable(self, mock_streamlit):
        """app.py 可正常导入。"""
        from workers.cr_analyze.dashboard import app

        assert hasattr(app, "main")

    def test_db_missing_shows_error(self, tmp_path, mock_streamlit):
        """SQLite 不存在时 components.check_db 返回 False。"""
        from workers.cr_analyze.dashboard.components import check_db

        fake_path = str(tmp_path / "nonexistent.db")
        assert check_db(fake_path) is False


def _load_db_direct(db_path: str) -> dict[str, pd.DataFrame]:
    """直接加载 SQLite 数据（绕过 Streamlit cache）。"""
    tables = {}
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        for (name,) in rows:
            tables[name] = pd.read_sql(f"SELECT * FROM [{name}]", conn)
    return tables
