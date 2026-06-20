# coding:utf8
"""tests/test_transformer.py -- OKR transformer 单元测试

使用 pytest 运行：
    source .venv/bin/activate
    pytest tests/test_transformer.py -v
"""
from __future__ import annotations

import pandas as pd
import pytest

from workers.okr.transformer import _aggregate_quality_reject


class TestAggregateQualityReject:
    """_aggregate_quality_reject 单元测试"""

    # ------------------------------------------------------------------
    # Fixtures
    # ------------------------------------------------------------------

    @pytest.fixture
    def sample_df_timestamp(self) -> pd.DataFrame:
        """飞书拉取的日期格式（pd.Timestamp）"""
        dates = pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-03", "2024-01-08"])
        return pd.DataFrame({
            "日期": dates,
            "商品id": ["A001", "A001", "A002", "A003", "A003", "A001"],
            "商品名称": ["商品A", "商品A", "商品B", "商品C", "商品C", "商品A"],
            "四级类目名称": ["香蕉", "香蕉", "西瓜", "葡萄", "葡萄", "香蕉"],
        })

    @pytest.fixture
    def sample_df_string(self) -> pd.DataFrame:
        """字符串日期格式（未转换的数据源）"""
        return pd.DataFrame({
            "日期": ["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-03", "2024-01-03", "2024-01-08"],
            "商品id": ["A001", "A001", "A002", "A003", "A003", "A001"],
            "商品名称": ["商品A", "商品A", "商品B", "商品C", "商品C", "商品A"],
            "四级类目名称": ["香蕉", "香蕉", "西瓜", "葡萄", "葡萄", "香蕉"],
        })

    @pytest.fixture
    def sample_df_empty(self) -> pd.DataFrame:
        """空 DataFrame"""
        return pd.DataFrame(columns=["日期", "商品id", "商品名称", "四级类目名称"])

    # ------------------------------------------------------------------
    # 测试用例
    # ------------------------------------------------------------------

    def test_basic_aggregation(self, sample_df_timestamp):
        """基础聚合：打回次数统计"""
        result = _aggregate_quality_reject(sample_df_timestamp)

        # 验证输出列
        assert "日期" in result.columns
        assert "打回水果次数" in result.columns

        # 验证聚合结果
        assert result.shape[0] == 4  # 4 个不同日期
        # 2024-01-01: 2次 (A001出现2次)
        # 2024-01-02: 1次
        # 2024-01-03: 2次 (A003出现2次)
        # 2024-01-08: 1次
        assert result[result["日期"] == pd.Timestamp("2024-01-01")]["打回水果次数"].values[0] == 2
        assert result[result["日期"] == pd.Timestamp("2024-01-02")]["打回水果次数"].values[0] == 1

    def test_7day_repeat_reject(self, sample_df_timestamp):
        """7日重复打回商品数统计"""
        result = _aggregate_quality_reject(sample_df_timestamp)

        # 验证输出列包含 7日重复打回商品数
        assert "7日重复打回商品数" in result.columns

        # 分析数据（7日窗口 = T-6 到 T0，即包含当天共7天）：
        # 2024-01-01: 窗口 12/26~01/01 → 仅 A001(01-01)，打回天=1，无重复
        # 2024-01-02: 窗口 12/27~01/02 → A001(01-01), A002(01-02)，各1天，无重复
        # 2024-01-03: 窗口 12/28~01/03 → A001(01-01), A002(01-02), A003(01-03)，各1天，无重复
        # 2024-01-08: 窗口 01/02~01/08 → A002(01-02), A003(01-03), A001(01-08)，各1天，无重复
        #             (注意：A001 的 01-01 不在窗口内)

        # 当前 sample 数据中，没有任何商品在7日窗口内出现超过1个不同打回日期
        row_0108 = result[result["日期"] == pd.Timestamp("2024-01-08")]
        assert row_0108["7日重复打回商品数"].values[0] == 0

    def test_timestamp_date_format(self, sample_df_timestamp):
        """pd.Timestamp 日期格式处理"""
        # 飞书拉取的数据，日期列应为 pd.Timestamp
        assert pd.api.types.is_datetime64_any_dtype(sample_df_timestamp["日期"])

        # 函数应正确处理，无需额外转换
        result = _aggregate_quality_reject(sample_df_timestamp)
        assert result.shape[0] > 0

    def test_string_date_format(self, sample_df_string):
        """字符串日期格式处理"""
        # 非 datetime64 类型，应进行转换
        assert not pd.api.types.is_datetime64_any_dtype(sample_df_string["日期"])

        result = _aggregate_quality_reject(sample_df_string)
        assert result.shape[0] > 0
        assert "打回水果次数" in result.columns
        assert "7日重复打回商品数" in result.columns

    def test_empty_dataframe(self, sample_df_empty):
        """空 DataFrame 处理"""
        result = _aggregate_quality_reject(sample_df_empty)
        # 空 DataFrame 应返回空聚合结果
        assert result.shape[0] == 0

    def test_group_by_cols_extensible(self, sample_df_timestamp):
        """group_by_cols 参数扩展性（未来支持多维度）"""
        # 当前默认按 ["日期"] 聚合
        result = _aggregate_quality_reject(sample_df_timestamp, group_by_cols=["日期"])
        assert result.columns.tolist() == ["日期", "打回水果次数", "打回水果商品数", "7日重复打回商品数"]

        # 未来扩展：按 ["日期", "四级类目名称"] 聚合
        result_multi = _aggregate_quality_reject(
            sample_df_timestamp, group_by_cols=["日期", "四级类目名称"]
        )
        assert "日期" in result_multi.columns
        assert "四级类目名称" in result_multi.columns
        assert "打回水果次数" in result_multi.columns
        # 7日重复打回商品数仅在 ["日期"] 维度时计算
        assert "7日重复打回商品数" not in result_multi.columns

    def test_no_repeat_in_window(self, sample_df_timestamp):
        """无重复打回场景"""
        # 创建一个各商品仅出现一次的 DataFrame
        df_no_repeat = pd.DataFrame({
            "日期": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "商品id": ["A001", "A002", "A003"],
            "商品名称": ["商品A", "商品B", "商品C"],
            "四级类目名称": ["香蕉", "西瓜", "葡萄"],
        })

        result = _aggregate_quality_reject(df_no_repeat)

        # 所有日期的 7日重复打回商品数 应为 0
        assert (result["7日重复打回商品数"] == 0).all()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])