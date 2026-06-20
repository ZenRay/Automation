# coding:utf8
"""E2E tests for workers.cr_trail_pricing pipeline (mocked Lark data)"""

import json
import tempfile
from datetime import date, timedelta

import pandas as pd
import pytest

from workers.cr_trail_pricing.transformer import (
    filter_trial_products,
    mark_trial_regions,
    associate_commission,
    associate_logistics_fee,
    compute_pricing,
    export_excel,
)
from workers.cr_trail_pricing.config import OUTPUT_COLUMNS

TARGET_DATE = date(2026, 6, 18)
YESTERDAY = TARGET_DATE - timedelta(days=1)


# ---------------------------------------------------------------------------
# Fixtures: mock data (field names match real Lark tables)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_conf_goods():
    """conf_商品信息 筛选昨日数据 (target_date - 1)"""
    return pd.DataFrame(
        {
            "日期": [YESTERDAY, YESTERDAY, YESTERDAY],
            "商城id": [1, 1, 1],
            "商品id": [101, 102, 103],
            "商品编码": ["SKU001", "SKU002", "SKU003"],
            "商品名称": ["商品A", "商品B", "商品C"],
            "后台类目名称": ["类目1", "类目2", "类目3"],
            "非试验区域抽佣率": [0.05, 0.08, 0.10],
            "毛重": [1.0, 2.0, 0.5],
        }
    )


@pytest.fixture
def mock_conf_trial_goods():
    """conf_试验商品信息: 仅 商品id + 日期 + 非试验区域抽佣率"""
    return pd.DataFrame(
        {
            "商品id": [101, 102, 200],
            "日期": [TARGET_DATE, TARGET_DATE, TARGET_DATE],
            "非试验区域抽佣率": [0.12, 0.15, 0.20],
        }
    )


@pytest.fixture
def mock_conf_county():
    return pd.DataFrame(
        {
            "日期": [TARGET_DATE] * 5,
            "试验区域id": [1001, 1002, 1003, 1004, 1005],
            "试验区域名称": ["区域A", "区域B", "区域C", "区域D", "区域E"],
            "修正区域名称": [""] * 5,
            "区域别名": [""] * 5,
            "地址id": [1, 2, 3, 4, 5],
            "地址名称": ["地址1", "地址2", "地址3", "地址4", "地址5"],
            "街道id": [1, 2, 3, 4, 5],
            "街道名称": ["街1", "街2", "街3", "街4", "街5"],
            "区县id": [100, 200, 300, 400, 500],
            "区县名称": ["县A", "县B", "县C", "县D", "县E"],
            "市id": [10, 20, 30, 40, 50],
            "市名称": ["市A", "市B", "市C", "市D", "市E"],
            "省id": [1, 2, 3, 4, 5],
            "省名称": ["省A", "省B", "省C", "省D", "省E"],
            "经度": ["0"] * 5,
            "纬度": ["0"] * 5,
            "父级区域id": [0] * 5,
            "是否有效": [1] * 5,
            "区域等级": [1] * 5,
            "试验区域类型": ["T"] * 5,
            "运营类型": ["类型A", "类型B", "类型A", "类型A", "类型A"],
        }
    )


@pytest.fixture
def mock_conf_trial_group():
    return pd.DataFrame(
        {
            "区域id": [10, 20],
            "区域名称": ["试验区域A", "试验区域B"],
            "区域类型": ["CITY", "CITY"],
            "试验分组": ["分组1", "分组2"],
            "试验起始日期": [date(2026, 6, 1), date(2026, 6, 1)],
            "试验结束日期": [date(2026, 6, 30), date(2026, 6, 30)],
        }
    )


@pytest.fixture
def mock_conf_trial_commission():
    return pd.DataFrame(
        {
            "试验阶段": ["阶段A", "阶段B"],
            "运营类型": ["类型A", "类型B"],
            "试验分组": ["分组1", "分组2"],
            "抽佣率": [0.03, 0.06],
            "试验起始日期": [date(2026, 6, 1), date(2026, 6, 1)],
            "试验结束日期": [date(2026, 6, 30), date(2026, 6, 30)],
        }
    )


@pytest.fixture
def mock_conf_hidden_logistics():
    return pd.DataFrame(
        {
            "日期": [TARGET_DATE, TARGET_DATE, TARGET_DATE],
            "商城id": [1, 1, 1],
            "市id": [10, 20, 30],
            "市名称": ["市A", "市B", "市C"],
            "费用类型": ["物流"] * 3,
            "费用计算方式": ["按比例"] * 3,
            "费率": [0.01, 0.02, 0.015],
            "区县费率映射": [
                json.dumps({"100": 0.005}),
                "",
                json.dumps({"999": 0.03}),
            ],
        }
    )


# ---------------------------------------------------------------------------
# Stage-level tests
# ---------------------------------------------------------------------------


class TestStage2FilterProducts:
    def test_inner_join_keeps_matching(self, mock_conf_goods, mock_conf_trial_goods):
        result = filter_trial_products(
            mock_conf_goods, mock_conf_trial_goods, TARGET_DATE
        )
        assert len(result) == 2
        assert set(result["商品id"]) == {101, 102}

    def test_keeps_only_product_fields(self, mock_conf_goods, mock_conf_trial_goods):
        result = filter_trial_products(
            mock_conf_goods, mock_conf_trial_goods, TARGET_DATE
        )
        assert list(result.columns) == [
            "商品id",
            "商品编码",
            "商品名称",
            "后台类目名称",
            "非试验区域抽佣率",
            "毛重",
            "是否试验区域",
        ]

    def test_commission_rate_from_trial_goods(
        self, mock_conf_goods, mock_conf_trial_goods
    ):
        """非试验区域抽佣率 取自 conf_试验商品信息，而非 conf_商品信息"""
        result = filter_trial_products(
            mock_conf_goods, mock_conf_trial_goods, TARGET_DATE
        )
        # 商品101: conf_goods=0.05, conf_trial_goods=0.12 → 应取 0.12
        row_101 = result[result["商品id"] == 101].iloc[0]
        assert row_101["非试验区域抽佣率"] == pytest.approx(0.12)
        # 商品102: conf_goods=0.08, conf_trial_goods=0.15 → 应取 0.15
        row_102 = result[result["商品id"] == 102].iloc[0]
        assert row_102["非试验区域抽佣率"] == pytest.approx(0.15)

    def test_is_trial_region_always_one(self, mock_conf_goods, mock_conf_trial_goods):
        result = filter_trial_products(
            mock_conf_goods, mock_conf_trial_goods, TARGET_DATE
        )
        assert (result["是否试验区域"] == 1).all()


class TestStage3MarkRegions:
    def test_marks_trial_and_non_trial(self, mock_conf_county, mock_conf_trial_group):
        result = mark_trial_regions(
            mock_conf_county, mock_conf_trial_group, TARGET_DATE
        )
        assert len(result) == 5
        trial = result[result["是否试验区域"] == 1]
        assert len(trial) == 2
        assert set(trial["市id"]) == {10, 20}

    def test_non_trial_has_empty_trial_fields(
        self, mock_conf_county, mock_conf_trial_group
    ):
        result = mark_trial_regions(
            mock_conf_county, mock_conf_trial_group, TARGET_DATE
        )
        non_trial = result[result["是否试验区域"] == 0]
        assert (non_trial["试验分组"] == "").all()

    def test_trial_region_id_from_county(self, mock_conf_county, mock_conf_trial_group):
        result = mark_trial_regions(
            mock_conf_county, mock_conf_trial_group, TARGET_DATE
        )
        trial = result[result["是否试验区域"] == 1]
        assert set(trial["试验区域id"]) == {1001, 1002}


class TestStage4AssociateCommission:
    def test_trial_gets_commission(
        self, mock_conf_county, mock_conf_trial_group, mock_conf_trial_commission
    ):
        regions = mark_trial_regions(
            mock_conf_county, mock_conf_trial_group, TARGET_DATE
        )
        result = associate_commission(regions, mock_conf_trial_commission, TARGET_DATE)
        trial = result[result["是否试验区域"] == 1]
        assert (trial["抽佣率"] > 0).all()

    def test_non_trial_has_nan_commission(
        self, mock_conf_county, mock_conf_trial_group, mock_conf_trial_commission
    ):
        """非试验区域未匹配抽佣表，抽佣率应为 NaN"""
        regions = mark_trial_regions(
            mock_conf_county, mock_conf_trial_group, TARGET_DATE
        )
        result = associate_commission(regions, mock_conf_trial_commission, TARGET_DATE)
        non_trial = result[result["是否试验区域"] == 0]
        assert non_trial["抽佣率"].isna().all()


class TestStage5LogisticsFee:
    def test_no_match_returns_nan(self):
        """未匹配物流表的行，隐形物流费率应为 NaN"""
        regions = pd.DataFrame(
            {
                "日期": [TARGET_DATE],
                "试验区域id": [1001],
                "试验区域名称": ["A"],
                "市id": [999],
                "省id": [1],
                "区县id": [100],
                "是否试验区域": [1],
                "抽佣率": [0.03],
            }
        )
        logistics = pd.DataFrame(
            {
                "日期": [TARGET_DATE],
                "商城id": [1],
                "市id": [10],
                "市名称": ["市A"],
                "费用类型": ["物流"],
                "费用计算方式": ["按比例"],
                "费率": [0.01],
                "区县费率映射": [""],
            }
        )
        result = associate_logistics_fee(regions, logistics, TARGET_DATE)
        assert pd.isna(result["隐形物流费率"].iloc[0])

    def test_empty_mapping_uses_city_rate(self):
        regions = pd.DataFrame(
            {
                "日期": [TARGET_DATE],
                "试验区域id": [1001],
                "试验区域名称": ["A"],
                "市id": [10],
                "省id": [1],
                "区县id": [100],
                "是否试验区域": [0],
                "抽佣率": [0],
            }
        )
        logistics = pd.DataFrame(
            {
                "日期": [TARGET_DATE],
                "商城id": [1],
                "市id": [10],
                "市名称": ["市A"],
                "费用类型": ["物流"],
                "费用计算方式": ["按比例"],
                "费率": [0.01],
                "区县费率映射": [""],
            }
        )
        result = associate_logistics_fee(regions, logistics, TARGET_DATE)
        assert result["隐形物流费率"].iloc[0] == pytest.approx(0.01)

    def test_county_in_mapping_uses_county_rate(self):
        regions = pd.DataFrame(
            {
                "日期": [TARGET_DATE],
                "试验区域id": [1001],
                "试验区域名称": ["A"],
                "市id": [10],
                "省id": [1],
                "区县id": [100],
                "是否试验区域": [0],
                "抽佣率": [0],
            }
        )
        logistics = pd.DataFrame(
            {
                "日期": [TARGET_DATE],
                "商城id": [1],
                "市id": [10],
                "市名称": ["市A"],
                "费用类型": ["物流"],
                "费用计算方式": ["按比例"],
                "费率": [0.01],
                "区县费率映射": [json.dumps({"100": 0.005})],
            }
        )
        result = associate_logistics_fee(regions, logistics, TARGET_DATE)
        assert result["隐形物流费率"].iloc[0] == pytest.approx(0.005)

    def test_county_not_in_mapping_uses_city_rate(self):
        regions = pd.DataFrame(
            {
                "日期": [TARGET_DATE],
                "试验区域id": [1001],
                "试验区域名称": ["A"],
                "市id": [10],
                "省id": [1],
                "区县id": [100],
                "是否试验区域": [0],
                "抽佣率": [0],
            }
        )
        logistics = pd.DataFrame(
            {
                "日期": [TARGET_DATE],
                "商城id": [1],
                "市id": [10],
                "市名称": ["市A"],
                "费用类型": ["物流"],
                "费用计算方式": ["按比例"],
                "费率": [0.01],
                "区县费率映射": [json.dumps({"999": 0.03})],
            }
        )
        result = associate_logistics_fee(regions, logistics, TARGET_DATE)
        assert result["隐形物流费率"].iloc[0] == pytest.approx(0.01)


class TestStage6Pricing:
    def _make_regions(self):
        return pd.DataFrame(
            {
                "日期": [TARGET_DATE, TARGET_DATE],
                "试验区域id": [1001, 1003],
                "试验区域名称": ["区域A", "区域C"],
                "市id": [10, 30],
                "省id": [1, 3],
                "区县id": [100, 300],
                "是否试验区域": [1, 0],
                "抽佣率": [0.03, 0],
                "隐形物流费率": [0.01, 0.02],
            }
        )

    def _make_products(self):
        return pd.DataFrame(
            {
                "商品id": [101],
                "商品编码": ["SKU001"],
                "商品名称": ["商品A"],
                "后台类目名称": ["类目1"],
                "非试验区域抽佣率": [0.05],
                "毛重": [1.0],
                "是否试验区域": [1],
            }
        )

    def test_cartesian_product_size(self):
        result = compute_pricing(self._make_products(), self._make_regions())
        assert len(result) == 2

    def test_trial_fixed_commission_ratio(self):
        result = compute_pricing(self._make_products(), self._make_regions())
        trial = result[result["是否试验区域"] == 1]
        # |抽佣率(0.03) - 非试验区域抽佣率(0.05)| = 0.02
        assert trial["固定抽佣比例"].iloc[0] == pytest.approx(0.02)
        assert pd.isna(trial["固定抽佣货值"].iloc[0])

    def test_non_trial_fixed_commission_value(self):
        result = compute_pricing(self._make_products(), self._make_regions())
        non_trial = result[result["是否试验区域"] == 0]
        assert non_trial["固定抽佣货值"].iloc[0] == pytest.approx(0.02 * 1.0)
        assert pd.isna(non_trial["固定抽佣比例"].iloc[0])

    def test_direction_trial_lower_commission(self):
        """抽佣率(0.03) < 非试验区域抽佣率(0.05) → 降价"""
        result = compute_pricing(self._make_products(), self._make_regions())
        trial = result[result["是否试验区域"] == 1]
        assert trial["调价方向"].iloc[0] == "降价"

    def test_direction_non_trial_always_up(self):
        result = compute_pricing(self._make_products(), self._make_regions())
        non_trial = result[result["是否试验区域"] == 0]
        assert non_trial["调价方向"].iloc[0] == "涨价"

    def test_direction_trial_higher_commission(self):
        """抽佣率(0.05) > 非试验区域抽佣率(0.02) → 涨价"""
        products = pd.DataFrame(
            {
                "商品id": [101],
                "商品编码": ["SKU001"],
                "商品名称": ["商品A"],
                "后台类目名称": ["类目1"],
                "非试验区域抽佣率": [0.02],
                "毛重": [1.0],
            }
        )
        regions = pd.DataFrame(
            {
                "日期": [TARGET_DATE],
                "试验区域id": [1001],
                "试验区域名称": ["区域A"],
                "市id": [10],
                "省id": [1],
                "区县id": [100],
                "是否试验区域": [1],
                "抽佣率": [0.05],
                "隐形物流费率": [0.01],
            }
        )
        result = compute_pricing(products, regions)
        assert result["调价方向"].iloc[0] == "涨价"

    def test_direction_trial_zero_diff(self):
        products = pd.DataFrame(
            {
                "商品id": [101],
                "商品编码": ["SKU001"],
                "商品名称": ["商品A"],
                "后台类目名称": ["类目1"],
                "非试验区域抽佣率": [0.03],
                "毛重": [1.0],
            }
        )
        regions = pd.DataFrame(
            {
                "日期": [TARGET_DATE],
                "试验区域id": [1001],
                "试验区域名称": ["区域A"],
                "市id": [10],
                "省id": [1],
                "区县id": [100],
                "是否试验区域": [1],
                "抽佣率": [0.03],
                "隐形物流费率": [0.01],
            }
        )
        result = compute_pricing(products, regions)
        assert result["调价方向"].iloc[0] == "不变"


class TestStage7Export:
    def test_output_columns_and_rename(self):
        df = pd.DataFrame(
            {
                "试验区域id": [1001],
                "试验区域名称": ["区域A"],
                "商品编码": ["SKU001"],
                "商品名称": ["商品A"],
                "调价方向": ["涨价"],
                "调价幅度": [float("nan")],
                "设置状态": ["启用"],
                "固定抽佣比例": [0.02],
                "固定抽佣货值": [float("nan")],
                "商品id": [101],
                "后台类目名称": ["类目1"],
            }
        )
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            export_excel(df, f.name)
            result = pd.read_excel(f.name)
        assert list(result.columns) == OUTPUT_COLUMNS
        # 确认临时检核字段不在正式输出中
        assert "市名称" not in result.columns
        assert "运营类型" not in result.columns
        # 固定抽佣比例 应已 × 100
        assert result["固定抽佣比例"].iloc[0] == pytest.approx(2.0)

    def test_empty_dataframe_writes_headers(self):
        df = pd.DataFrame(
            columns=[
                "试验区域id",
                "试验区域名称",
                "商品编码",
                "商品名称",
                "调价方向",
                "调价幅度",
                "设置状态",
                "固定抽佣比例",
                "固定抽佣货值",
                "商品id",
                "后台类目名称",
            ]
        )
        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            export_excel(df, f.name)
            result = pd.read_excel(f.name)
        assert list(result.columns) == OUTPUT_COLUMNS
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Full pipeline test
# ---------------------------------------------------------------------------


class TestFullPipeline:
    def test_full_pipeline(
        self,
        mock_conf_goods,
        mock_conf_trial_goods,
        mock_conf_county,
        mock_conf_trial_group,
        mock_conf_trial_commission,
        mock_conf_hidden_logistics,
    ):
        products = filter_trial_products(
            mock_conf_goods, mock_conf_trial_goods, TARGET_DATE
        )
        regions = mark_trial_regions(
            mock_conf_county, mock_conf_trial_group, TARGET_DATE
        )
        regions = associate_commission(regions, mock_conf_trial_commission, TARGET_DATE)
        regions = associate_logistics_fee(
            regions, mock_conf_hidden_logistics, TARGET_DATE
        )
        result = compute_pricing(products, regions)

        # 过滤后：试验行(4) + 有物流匹配的非试验行(2) = 6
        # 市id=40,50 的非试验行两者皆空被过滤
        assert len(result) == 6

        trial_rows = result[result["是否试验区域"] == 1]
        non_trial_rows = result[result["是否试验区域"] == 0]
        assert len(trial_rows) == 2 * 2
        assert len(non_trial_rows) == 2  # 只有匹配物流表的非试验行保留

        assert (trial_rows["固定抽佣比例"].notna()).all()
        assert (trial_rows["固定抽佣货值"].isna()).all()
        assert (non_trial_rows["固定抽佣比例"].isna()).all()
        # 过滤后，保留的非试验行全部有 固定抽佣货值
        assert (non_trial_rows["固定抽佣货值"].notna()).all()
        assert (result["设置状态"] == "启用").all()

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            output = export_excel(result, f.name)
            assert list(output.columns) == OUTPUT_COLUMNS
            excel_data = pd.read_excel(f.name)
            assert len(excel_data) == 6

    def test_empty_products(
        self,
        mock_conf_trial_goods,
        mock_conf_county,
        mock_conf_trial_group,
        mock_conf_trial_commission,
        mock_conf_hidden_logistics,
    ):
        empty_goods = pd.DataFrame(
            columns=[
                "日期",
                "商城id",
                "商品id",
                "商品编码",
                "商品名称",
                "后台类目名称",
                "非试验区域抽佣率",
                "毛重",
            ]
        )
        products = filter_trial_products(
            empty_goods, mock_conf_trial_goods, TARGET_DATE
        )
        assert len(products) == 0

        regions = mark_trial_regions(
            mock_conf_county, mock_conf_trial_group, TARGET_DATE
        )
        regions = associate_commission(regions, mock_conf_trial_commission, TARGET_DATE)
        regions = associate_logistics_fee(
            regions, mock_conf_hidden_logistics, TARGET_DATE
        )
        result = compute_pricing(products, regions)
        assert len(result) == 0

        with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as f:
            output = export_excel(result, f.name)
            assert len(output) == 0
