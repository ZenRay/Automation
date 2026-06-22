# coding:utf8
"""tests/cr_analyze/conftest.py -- 共享 fixtures"""

import pytest
import pandas as pd
from datetime import date


@pytest.fixture
def sample_product_info() -> pd.DataFrame:
    return pd.DataFrame({
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
    })


@pytest.fixture
def sample_county_info() -> pd.DataFrame:
    return pd.DataFrame({
        "日期": [date(2026, 6, 20)] * 4,
        "区县id": [110101, 110102, 430201, 430501],
        "区县名称": ["东城区", "西城区", "荷塘区", "双清区"],
        "市id": [110100, 110100, 430200, 430500],
        "市名称": ["北京市辖区", "北京市辖区", "株洲市", "邵阳市"],
        "省id": [110000, 110000, 430000, 430000],
        "省名称": ["北京市", "北京市", "湖南省", "湖南省"],
        "运营类型": ["自营区域", "代理人区域", "自营区域", "代理人区域"],
    })


@pytest.fixture
def sample_commission_adjustment() -> pd.DataFrame:
    return pd.DataFrame({
        "日期": [date(2026, 6, 20)] * 4,
        "商品id": [10184690, 10184690, 20519020, 20588413],
        "区县名称": ["东城区", "荷塘区", "西城区", "双清区"],
        "区域全称": ["北京市-东城区", "湖南省-株洲市-荷塘区", "北京市-西城区", "湖南省-邵阳市-双清区"],
        "调整系数": [1.0, 1.0, 1.0, 1.0],
        "固定抽佣率调整": [0.0, 0.02, 0.0, 0.035],
        "固定抽佣金额调整": [0.0, 12.0, 0.0, 15.0],
        "参与试验类型": ["[试验区域]", "[试验区域]", "[非试验区域]", "[试验区域]"],
    })


@pytest.fixture
def sample_trial_group() -> pd.DataFrame:
    return pd.DataFrame({
        "区域id": [110100, 430200, 430500, 830300],
        "区域名称": ["北京市辖区", "株洲市", "邵阳市", "萍乡市2"],
        "市名称": ["北京市辖区", "株洲市", "邵阳市", "萍乡市"],
        "区域类型": ["CITY", "CITY", "CITY", "CITY"],
        "试验分组": ["对照组", "试验组一", "试验组二", "试验组三"],
        "试验起始日期": [date(2026, 6, 19)] * 4,
        "试验结束日期": [date(2026, 7, 19)] * 4,
    })


@pytest.fixture
def sample_trial_period_rate() -> pd.DataFrame:
    return pd.DataFrame({
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
    })


@pytest.fixture
def sample_trial_region_price() -> pd.DataFrame:
    return pd.DataFrame({
        "日期": [date(2026, 6, 20)] * 2,
        "商品id": [10184690, 20519020],
        "商品名称": ["云南水仙芒大果", "云南水仙芒中果"],
        "商家名称": ["得兴果业", "得兴果业"],
        "区域全称": ["湖南省-长沙市", "湖南省-岳阳市"],
        "试验区域平台销售斤单价": [6.5, 6.0],
        "试验区域平台销售件单价": [150.0, 135.0],
        "试验区域商家供货斤单价": [5.8, 5.4],
        "试验区域商家供货件单价": [134.0, 121.0],
        "抽佣率": [0.095, 0.095],
    })


@pytest.fixture
def sample_fact_order_item() -> pd.DataFrame:
    return pd.DataFrame({
        "日期": [date(2026, 6, 20)] * 6,
        "订单id": ["o1", "o2", "o3", "o4", "o5", "o6"],
        "明细订单id": ["oi1", "oi2", "oi3", "oi4", "oi5", "oi6"],
        "商品id": [10184690, 10184690, 20519020, 20519020, 20588413, 99999999],
        "商品名称": ["水仙芒A"] * 6,
        "商家名称": ["商家A"] * 6,
        "实际抽佣率": [0.08] * 6,
        "商家供货斤单价": [5.5, 5.5, 5.0, 5.0, 4.5, 4.0],
        "商家供货件单价": [130.0] * 6,
        "平台销售斤单价": [6.0] * 6,
        "平台销售件单价": [140.0] * 6,
        "店铺id": ["s1", "s2", "s1", "s3", "s2", "s4"],
        "区县id": [110101, 430201, 430501, 110102, 430201, 110101],
        "区县名称": ["东城区", "荷塘区", "双清区", "西城区", "荷塘区", "东城区"],
        "下单数量": [10, 8, 12, 6, 15, 5],
        "送达金额": [140.0, 112.0, 180.0, 90.0, 225.0, 75.0],
        "送达数量": [10, 8, 10, 6, 15, 5],
        "送达抽佣金额": [11.2, 8.96, 14.4, 7.2, 18.0, 6.0],
        "是否有效订单": [1, 1, 1, 1, 1, 0],
    })


@pytest.fixture
def sample_lark_data(
    sample_product_info,
    sample_county_info,
    sample_commission_adjustment,
    sample_trial_group,
    sample_trial_period_rate,
    sample_trial_region_price,
) -> dict:
    return {
        "conf_product_info": sample_product_info,
        "conf_county_info": sample_county_info,
        "conf_commission_adjustment": sample_commission_adjustment,
        "conf_trial_group": sample_trial_group,
        "conf_trial_period_rate": sample_trial_period_rate,
        "conf_trial_region_price": sample_trial_region_price,
    }


@pytest.fixture
def sample_mc_data(sample_fact_order_item) -> dict:
    return {"fact_order_item": sample_fact_order_item}
