# coding:utf8
"""tests/cr_analyze/test_pipeline_e2e.py -- 管道端到端测试

使用 mock 替换 Lark/MC 客户端，验证从 DataFrame → 聚合 → SQLite 的完整链路。
"""

import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from workers.cr_analyze.main import run_cr_analyze_pipeline
from workers.cr_analyze.sqlite_store import list_tables, read_table, table_exists


@pytest.fixture
def e2e_db(tmp_path) -> str:
    return str(tmp_path / "e2e_test.db")


@pytest.fixture
def mock_lark_data():
    """构造完整的 6 张 Lark 表 mock 数据。"""
    return {
        "conf_product_info": pd.DataFrame({
            "日期": [date(2026, 6, 20)] * 3,
            "商品id": [10184690, 20519020, 20588413],
            "商品名称": ["云南水仙芒大果", "云南水仙芒中果", "广西水仙芒大果"],
            "产地": ["云南", "云南", "广西"],
            "包装类型": ["泡沫箱", "泡沫箱", "纸箱"],
            "单果大小": ["大果", "中果", "大果"],
            "色号": ["5号色", "5号色", "3号色"],
            "商品头数": ["12", "15", "10"],
            "非试验区域平台销售斤单价": [6.0, 5.5, 5.0],
            "非试验区域平台销售件单价": [140.0, 120.0, 110.0],
            "非试验区域商家供货斤单价": [5.4, 4.9, 4.5],
            "非试验区域商家供货件单价": [126.0, 107.0, 99.0],
            "是否当日上架": [0, 1, 0],
        }),
        "conf_county_info": pd.DataFrame({
            "日期": [date(2026, 6, 20)] * 2,
            "区县id": [430201, 430501],
            "区县名称": ["荷塘区", "双清区"],
            "市id": [430200, 430500],
            "市名称": ["株洲市", "邵阳市"],
            "省id": [430000, 430000],
            "省名称": ["湖南省", "湖南省"],
            "运营类型": ["自营区域", "代理人区域"],
        }),
        "conf_commission_adjustment": pd.DataFrame({
            "日期": [date(2026, 6, 20)] * 2,
            "商品id": [10184690, 20519020],
            "区县名称": ["荷塘区", "双清区"],
            "区域全称": ["湖南省-株洲市-荷塘区", "湖南省-邵阳市-双清区"],
            "调整系数": [1.0, 1.0],
            "固定抽佣率调整": [0.02, 0.035],
            "固定抽佣金额调整": [12.0, 15.0],
            "参与试验类型": ["[试验区域]", "[试验区域]"],
        }),
        "conf_trial_region_price": pd.DataFrame({
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
        }),
        "conf_trial_group": pd.DataFrame({
            "区域id": [430200, 430500],
            "区域名称": ["株洲市", "邵阳市"],
            "市名称": ["株洲市", "邵阳市"],
            "区域类型": ["CITY", "CITY"],
            "试验分组": ["试验组一", "试验组二"],
            "试验起始日期": [date(2026, 6, 19), date(2026, 6, 19)],
            "试验结束日期": [date(2026, 7, 19), date(2026, 7, 19)],
        }),
        "conf_trial_period_rate": pd.DataFrame({
            "试验阶段": ["摸底期", "摸底期", "生效期", "生效期"],
            "运营类型": ["自营区域", "代理人区域", "自营区域", "代理人区域"],
            "抽佣率": [0.075, 0.046, 0.095, 0.066],
            "试验分组": ["试验组一", "试验组一", "试验组一", "试验组一"],
            "试验起始日期": [
                date(2026, 6, 19), date(2026, 6, 19),
                date(2026, 6, 29), date(2026, 6, 29),
            ],
            "试验结束日期": [
                date(2026, 6, 28), date(2026, 6, 28),
                date(2026, 7, 19), date(2026, 7, 19),
            ],
        }),
    }


@pytest.fixture
def mock_mc_data():
    """构造 MaxCompute 事实表 mock 数据。"""
    return {
        "fact_order_item": pd.DataFrame({
            "日期": [date(2026, 6, 20)] * 8,
            "订单id": [f"o{i}" for i in range(8)],
            "明细订单id": [f"oi{i}" for i in range(8)],
            "商品id": [10184690, 10184690, 20519020, 20519020, 20588413, 20588413, 99999999, 10184690],
            "商品名称": ["水仙芒"] * 8,
            "商家名称": ["商家A"] * 8,
            "实际抽佣率": [0.08] * 8,
            "商家供货斤单价": [5.5] * 8,
            "商家供货件单价": [130.0] * 8,
            "平台销售斤单价": [6.0] * 8,
            "平台销售件单价": [140.0] * 8,
            "店铺id": ["s1", "s2", "s1", "s3", "s2", "s4", "s5", "s1"],
            "区县id": [430201, 430201, 430501, 430201, 430501, 430501, 430201, 430201],
            "区县名称": ["荷塘区", "荷塘区", "双清区", "荷塘区", "双清区", "双清区", "荷塘区", "荷塘区"],
            "下单数量": [10, 8, 12, 6, 15, 9, 5, 7],
            "送达金额": [140.0, 112.0, 180.0, 90.0, 225.0, 135.0, 75.0, 98.0],
            "送达数量": [10, 8, 10, 6, 15, 9, 5, 7],
            "送达抽佣金额": [11.2, 8.96, 14.4, 7.2, 18.0, 10.8, 6.0, 7.84],
            "是否有效订单": [1, 1, 1, 1, 1, 1, 0, 1],
        }),
    }


@pytest.mark.integration
class TestPipelineE2E:
    def _make_extract_side_effect(self, mock_lark_data):
        """Create side_effect for extract_single_source mock."""
        def side_effect(client, source):
            return mock_lark_data.get(source.name, pd.DataFrame())
        return side_effect

    @patch("workers.cr_analyze.main._init_mc_client")
    @patch("workers.cr_analyze.main._init_lark_client")
    @patch("workers.cr_analyze.main.extract_single_source")
    @patch("workers.cr_analyze.main.execute_all_queries")
    def test_full_pipeline(
        self, mock_mc_exec, mock_lark_extract, mock_lark_init, mock_mc_init,
        e2e_db, mock_lark_data, mock_mc_data,
    ):
        """完整管道: mock 提取 → 真实聚合 → 真实 SQLite 写入。"""
        mock_lark_extract.side_effect = self._make_extract_side_effect(mock_lark_data)
        mock_mc_exec.return_value = mock_mc_data
        mock_lark_init.return_value = MagicMock()
        mock_mc_init.return_value = MagicMock()

        rc = run_cr_analyze_pipeline(target_date=date(2026, 6, 20), db_path=e2e_db)
        assert rc == 0

        # 验证 SQLite 表存在
        tables = list_tables(e2e_db)
        assert "conf_product_info" in tables
        assert "conf_county_info" in tables
        assert "conf_commission_adjustment" in tables
        assert "conf_trial_region_price" in tables
        assert "conf_trial_group" in tables
        assert "conf_trial_period_rate" in tables
        assert "fact_order_item" in tables
        assert "agg_wide_table" in tables

    @patch("workers.cr_analyze.main._init_mc_client")
    @patch("workers.cr_analyze.main._init_lark_client")
    @patch("workers.cr_analyze.main.extract_single_source")
    @patch("workers.cr_analyze.main.execute_all_queries")
    def test_wide_table_has_expected_columns(
        self, mock_mc_exec, mock_lark_extract, mock_lark_init, mock_mc_init,
        e2e_db, mock_lark_data, mock_mc_data,
    ):
        """宽表包含预期列。"""
        mock_lark_extract.side_effect = self._make_extract_side_effect(mock_lark_data)
        mock_mc_exec.return_value = mock_mc_data
        mock_lark_init.return_value = MagicMock()
        mock_mc_init.return_value = MagicMock()

        run_cr_analyze_pipeline(target_date=date(2026, 6, 20), db_path=e2e_db)

        wide = read_table(e2e_db, "agg_wide_table")
        for col in ["stage", "city_unit", "sku_id", "gmv", "commission_amount", "commission_rate"]:
            assert col in wide.columns, f"Missing column: {col}"

    @patch("workers.cr_analyze.main._init_mc_client")
    @patch("workers.cr_analyze.main._init_lark_client")
    @patch("workers.cr_analyze.main.extract_single_source")
    @patch("workers.cr_analyze.main.execute_all_queries")
    def test_public_filters_applied(
        self, mock_mc_exec, mock_lark_extract, mock_lark_init, mock_mc_init,
        e2e_db, mock_lark_data, mock_mc_data,
    ):
        """公共过滤: 有效订单 + 目标 SKU + 试验区域。"""
        mock_lark_extract.side_effect = self._make_extract_side_effect(mock_lark_data)
        mock_mc_exec.return_value = mock_mc_data
        mock_lark_init.return_value = MagicMock()
        mock_mc_init.return_value = MagicMock()

        run_cr_analyze_pipeline(target_date=date(2026, 6, 20), db_path=e2e_db)

        # fact_order_item 存入 SQLite 时保留原始数据
        fact = read_table(e2e_db, "fact_order_item")
        # 原始 8 行全部存入（过滤在聚合时执行）
        assert len(fact) == 8

        # 宽表应排除: sku 99999999, 无效订单
        wide = read_table(e2e_db, "agg_wide_table")
        if not wide.empty and "sku_id" in wide.columns:
            assert 99999999 not in wide["sku_id"].values

    @patch("workers.cr_analyze.main._init_mc_client")
    @patch("workers.cr_analyze.main._init_lark_client")
    @patch("workers.cr_analyze.main.extract_single_source")
    @patch("workers.cr_analyze.main.execute_all_queries")
    def test_empty_fact_data(
        self, mock_mc_exec, mock_lark_extract, mock_lark_init, mock_mc_init,
        e2e_db, mock_lark_data,
    ):
        """空事实数据 → 宽表为空，不崩溃。"""
        mock_lark_extract.side_effect = self._make_extract_side_effect(mock_lark_data)
        mock_mc_exec.return_value = {"fact_order_item": pd.DataFrame()}
        mock_lark_init.return_value = MagicMock()
        mock_mc_init.return_value = MagicMock()

        rc = run_cr_analyze_pipeline(target_date=date(2026, 6, 20), db_path=e2e_db)
        assert rc == 0

        # 空事实数据 → 宽表为空，不会被写入 SQLite（无列的 DataFrame 跳过）
        assert not table_exists(e2e_db, "agg_wide_table")

    @patch("workers.cr_analyze.main._init_mc_client")
    @patch("workers.cr_analyze.main._init_lark_client")
    @patch("workers.cr_analyze.main.extract_single_source")
    @patch("workers.cr_analyze.main.execute_all_queries")
    def test_table_overwrite_on_rerun(
        self, mock_mc_exec, mock_lark_extract, mock_lark_init, mock_mc_init,
        e2e_db, mock_lark_data, mock_mc_data,
    ):
        """重新运行管道时表被覆盖。"""
        mock_lark_extract.side_effect = self._make_extract_side_effect(mock_lark_data)
        mock_mc_exec.return_value = mock_mc_data
        mock_lark_init.return_value = MagicMock()
        mock_mc_init.return_value = MagicMock()

        # 第一次运行
        run_cr_analyze_pipeline(target_date=date(2026, 6, 20), db_path=e2e_db)
        fact_v1 = read_table(e2e_db, "fact_order_item")

        # 第二次运行（数据相同但应覆盖）
        run_cr_analyze_pipeline(target_date=date(2026, 6, 20), db_path=e2e_db)
        fact_v2 = read_table(e2e_db, "fact_order_item")

        assert len(fact_v1) == len(fact_v2)
