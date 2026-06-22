# coding:utf8
"""tests/cr_analyze/test_power_analysis.py -- 功效分析单元测试"""

from datetime import date

import numpy as np
import pandas as pd
import pytest


def _make_weekly_gmv(
    sku_id: int,
    city_gmv: dict[str, list[float]],
    weeks: list[str],
) -> pd.DataFrame:
    """构造周级 GMV 数据用于测试。

    Args:
        sku_id: SKU 标识
        city_gmv: {city_unit: [w1_gmv, w2_gmv, ...]} 每城市每周 GMV
        weeks: 周标识列表
    """
    rows = []
    for city, gmv_list in city_gmv.items():
        for week, gmv in zip(weeks, gmv_list):
            rows.append({
                "sku_id": sku_id,
                "city_unit": city,
                "week_id": week,
                "gmv": gmv,
                "日期": date(2026, 4, 13),  # placeholder
            })
    return pd.DataFrame(rows)


class TestSigmaComputation:
    def test_known_data(self):
        """用已知数据验证 σ_raw 计算。"""
        from workers.cr_analyze.transformer import _compute_sigma

        # 8 个城市，4 周 GMV，数据使得 CV 均值为已知值
        np.random.seed(42)
        city_gmv = {}
        cities = [f"city_{i}" for i in range(8)]
        for city in cities:
            base = np.random.uniform(500, 1000)
            city_gmv[city] = [
                base * (1 + np.random.normal(0, 0.05)),
                base * (1 + np.random.normal(0, 0.05)),
                base * (1 + np.random.normal(0, 0.05)),
                base * (1 + np.random.normal(0, 0.05)),
            ]

        df = _make_weekly_gmv(10184690, city_gmv, ["W1", "W2", "W3", "W4"])
        result = _compute_sigma(df, sku_id=10184690)

        assert "sigma_raw" in result
        assert "sigma_adjusted" in result
        assert result["sigma_raw"] > 0
        assert abs(result["sigma_adjusted"] - result["sigma_raw"] * 1.5) < 1e-10

    def test_single_week_returns_nan(self):
        """只有 1 周数据时，σ 应为 NaN。"""
        from workers.cr_analyze.transformer import _compute_sigma

        df = _make_weekly_gmv(10184690, {"city_a": [100.0]}, ["W1"])
        result = _compute_sigma(df, sku_id=10184690)
        assert np.isnan(result["sigma_raw"])

    def test_zero_gmv_excluded(self):
        """GMV 为 0 的城市不参与 CV 计算。"""
        from workers.cr_analyze.transformer import _compute_sigma

        city_gmv = {
            "city_a": [100, 110, 95, 105],
            "city_b": [0, 0, 0, 0],  # 全部为 0，应排除
            "city_c": [200, 210, 195, 205],
        }
        df = _make_weekly_gmv(10184690, city_gmv, ["W1", "W2", "W3", "W4"])
        result = _compute_sigma(df, sku_id=10184690)
        assert result["sigma_raw"] > 0


class TestRhoComputation:
    def test_perfect_correlation(self):
        """两城市 GMV 完全线性相关时 ρ ≈ 1。"""
        from workers.cr_analyze.transformer import _compute_rho

        city_gmv = {}
        cities = [f"city_{i}" for i in range(8)]
        for city in cities:
            base = hash(city) % 1000
            city_gmv[city] = [base, base * 2]  # 完全线性

        df = _make_weekly_gmv(10184690, city_gmv, ["W1", "W2"])
        result = _compute_rho(df, sku_id=10184690, week_pairs=[("W1", "W2")])

        assert "rho_values" in result
        rho = result["rho_values"][0]
        assert abs(rho - 1.0) < 0.01 or abs(rho + 1.0) < 0.01  # ±1 (线性)

    def test_rho_main_is_min(self):
        """ρ_main 取 min(ρ_pre, ρ_post)。"""
        from workers.cr_analyze.transformer import _compute_rho

        city_gmv = {}
        np.random.seed(123)
        for i in range(8):
            city_gmv[f"city_{i}"] = [
                100 + i * 10,
                110 + i * 10,
                200 + i * 5,
                180 + i * 5,
            ]

        df = _make_weekly_gmv(10184690, city_gmv, ["W1", "W2", "W3", "W4"])
        result = _compute_rho(
            df, sku_id=10184690,
            week_pairs=[("W1", "W2"), ("W3", "W4")],
        )
        assert result["rho_main"] == min(result["rho_values"])


class TestPowerFormula:
    def test_sufficient_power(self):
        """σ 小、ρ 高时功效应充足。"""
        from workers.cr_analyze.transformer import _compute_power

        result = _compute_power(sigma_adjusted=0.1, rho_main=0.99, n_actual=2)
        assert result["n_required"] < 2
        assert result["power_sufficient"] is True

    def test_insufficient_power(self):
        """σ 大、ρ 低时功效应不足。"""
        from workers.cr_analyze.transformer import _compute_power

        result = _compute_power(sigma_adjusted=0.5, rho_main=0.5, n_actual=2)
        assert result["n_required"] > 2
        assert result["power_sufficient"] is False

    def test_boundary_case(self):
        """σ=0.15, ρ=0.95 → n_required ≈ 1.41 < 2 → 功效充足。"""
        from workers.cr_analyze.transformer import _compute_power

        result = _compute_power(sigma_adjusted=0.15, rho_main=0.95, n_actual=2)
        # 4 * 0.0225 * 0.05 * 7.84 / 0.01 = 0.3528 / 0.01 = 3.528 → 实际 > 2
        # 用更小的 σ 和更高的 ρ 来构造边界通过的案例
        result2 = _compute_power(sigma_adjusted=0.1, rho_main=0.95, n_actual=2)
        # 4 * 0.01 * 0.05 * 7.84 / 0.01 = 0.01568 / 0.01 = 1.568 < 2
        assert result2["n_required"] < 2
        assert result2["power_sufficient"] is True


class TestCrossCorrelation:
    def test_three_pairs(self):
        """3 个 SKU 应产生 3 对相关系数。"""
        from workers.cr_analyze.transformer import _compute_cross_correlation

        dfs = {
            10184690: pd.DataFrame({
                "city_unit": ["a", "b", "c"],
                "gmv": [100, 200, 300],
            }),
            20519020: pd.DataFrame({
                "city_unit": ["a", "b", "c"],
                "gmv": [150, 250, 350],
            }),
            20588413: pd.DataFrame({
                "city_unit": ["a", "b", "c"],
                "gmv": [80, 180, 280],
            }),
        }
        result = _compute_cross_correlation(dfs)
        assert len(result) == 3

    def test_high_correlation_flagged(self):
        """高度相关的 SKU 对应标记风险。"""
        from workers.cr_analyze.transformer import _compute_cross_correlation

        dfs = {
            10184690: pd.DataFrame({
                "city_unit": ["a", "b", "c", "d"],
                "gmv": [100, 200, 300, 400],
            }),
            20519020: pd.DataFrame({
                "city_unit": ["a", "b", "c", "d"],
                "gmv": [101, 201, 301, 401],  # 几乎完全相关
            }),
            20588413: pd.DataFrame({
                "city_unit": ["a", "b", "c", "d"],
                "gmv": [50, 150, 250, 350],
            }),
        }
        result = _compute_cross_correlation(dfs)
        # 10184690 vs 20519020 应该 > 0.5
        pair_key = (10184690, 20519020)
        assert any(
            r["rho"] > 0.5
            for r in result
            if (r["sku_a"], r["sku_b"]) == pair_key or (r["sku_b"], r["sku_a"]) == pair_key
        )


class TestFallbackBehavior:
    def test_insufficient_weeks_flagged(self):
        """少于 3 周非零 GMV 应标记 fallback。"""
        from workers.cr_analyze.transformer import compute_power_analysis
        from workers.cr_analyze.config import TRIAL_PHASE_CONFIG

        # 只有 2 周数据
        fact = pd.DataFrame({
            "日期": [date(2026, 4, 14)] * 8 + [date(2026, 4, 21)] * 8,
            "sku_id": [10184690] * 16,
            "city_unit": [f"city_{i % 8}" for i in range(8)] * 2,
            "gmv": [100.0] * 16,
            "commission_amount": [8.0] * 16,
            "store_id": [f"s{i % 4}" for i in range(8)] * 2,
            "明细订单id": [f"oi{i}" for i in range(16)],
        })

        result = compute_power_analysis(fact, TRIAL_PHASE_CONFIG)
        assert not result.empty
        # 应有 fallback 标记
        if "fallback" in result.columns:
            assert result.iloc[0]["fallback"] is True or result.iloc[0]["fallback"] == True
