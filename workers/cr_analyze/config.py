# coding:utf8
"""workers.cr_analyze.config -- 抽佣率试验分析 唯一配置注入点

所有业务参数（飞书表配置、SQL 查询、字段映射、试验阶段参数、告警阈值）
均在此定义，供 main.py 和 transformer.py 使用。
"""

from datetime import date
from pathlib import Path

from workers.lib import LarkSourceConfig, SQLQueryConfig

# --------------------------------------------------------------------------
# SQL 文件目录
# --------------------------------------------------------------------------
SQL_BASE_DIR = Path(__file__).parent / "sql"

# --------------------------------------------------------------------------
# 飞书 Wiki Base URL（6 张配置表共享）
# --------------------------------------------------------------------------
_WIKI_BASE = "https://bggc.feishu.cn/wiki/TcALwGgnciCQQYkPeHYcYf1Cnkd"

# --------------------------------------------------------------------------
# 飞书数据源配置：6 张配置表
# --------------------------------------------------------------------------
LARK_SOURCES: list[LarkSourceConfig] = [
    LarkSourceConfig(
        name="conf_product_info",
        url=f"{_WIKI_BASE}?table=tblevDYqsTdwu8fo&view=default",
        table_name="conf_商品信息",
        field_names=[
            "日期",
            "商品id",
            "商品名称",
            "产地",
            "包装类型",
            "单果大小",
            "色号",
            "商品头数",
            "非试验区域平台销售斤单价",
            "非试验区域平台销售件单价",
            "非试验区域商家供货斤单价",
            "非试验区域商家供货件单价",
            "是否当日上架",
        ],
        date_filter_field=None,
        date_fields=["日期"],
    ),
    LarkSourceConfig(
        name="conf_county_info",
        url=f"{_WIKI_BASE}?table=tblBgJYpBRT18Uvp&view=default",
        table_name="conf_区县信息",
        field_names=[
            "日期",
            "区县id",
            "区县名称",
            "市id",
            "市名称",
            "省id",
            "省名称",
            "运营类型",
        ],
        date_filter_field="日期",
        date_fields=["日期"],
    ),
    LarkSourceConfig(
        name="conf_commission_adjustment",
        url=f"{_WIKI_BASE}?table=tbl1Wa88og2jX26R&view=default",
        table_name="conf_线上商品区域抽佣率调整",
        field_names=[
            "日期",
            "商品id",
            "区县名称",
            "区域全称",
            "调整系数",
            "固定抽佣率调整",
            "固定抽佣金额调整",
            "参与试验类型",
        ],
        date_filter_field="日期",
        date_fields=["日期"],
        date_filter_start_date=date(2026, 6, 19),
    ),
    LarkSourceConfig(
        name="conf_trial_region_price",
        url=f"{_WIKI_BASE}?table=tbl4nwTsRUZSubLF&view=default",
        table_name="conf_线上商品试验区域价格",
        field_names=[
            "日期",
            "商品id",
            "商品名称",
            "商家名称",
            "区域全称",
            "试验区域平台销售斤单价",
            "试验区域平台销售件单价",
            "试验区域商家供货斤单价",
            "试验区域商家供货件单价",
            "抽佣率",
        ],
        date_filter_field="日期",
        date_fields=["日期"],
        date_filter_start_date=date(2026, 6, 19),
    ),
    LarkSourceConfig(
        name="conf_trial_group",
        url=f"{_WIKI_BASE}?table=tbl2hCVkpjtMt16J&view=default",
        table_name="conf_试验分组配置",
        field_names=[
            "区域id",
            "区域名称",
            "市名称",
            "区域类型",
            "试验分组",
            "试验起始日期",
            "试验结束日期",
        ],
        date_filter_field=None,
        date_fields=["试验起始日期", "试验结束日期"],
    ),
    LarkSourceConfig(
        name="conf_trial_period_rate",
        url=f"{_WIKI_BASE}?table=tblXeBNiHArKWmXm&view=default",
        table_name="conf_试验周期抽佣率",
        field_names=[
            "试验阶段",
            "运营类型",
            "抽佣率",
            "试验分组",
            "试验起始日期",
            "试验结束日期",
        ],
        date_filter_field=None,
        date_fields=["试验起始日期", "试验结束日期"],
    ),
]

# --------------------------------------------------------------------------
# MaxCompute SQL 查询配置
# --------------------------------------------------------------------------
SQL_QUERIES: list[SQLQueryConfig] = [
    SQLQueryConfig(
        name="fact_order_item",
        sql_file="order_fact_whole.sql",
        depends_on=[],
        use_temp_table=True,
        temp_table_project="datawarehouse_max_dev",
    ),
]

# --------------------------------------------------------------------------
# SQL 输出列 → 宽表字段映射
# --------------------------------------------------------------------------
FIELD_MAPPING: dict[str, str] = {
    "日期": "日期",
    "商品id": "sku_id",
    "明细订单id": "order_item_id",
    "店铺id": "store_id",
    "送达金额": "gmv",
    "送达抽佣金额": "commission_amount",
    "商家供货斤单价": "supply_price_per_jin",
    "区县id": "county_id",
    "区县名称": "county_name",
    "下单数量": "ordered_num",
    "送达数量": "delivered_num",
    "是否有效订单": "is_valid",
}

# --------------------------------------------------------------------------
# 目标 SKU 列表（水仙芒试验商品）
# --------------------------------------------------------------------------
TARGET_SKU_IDS: list[int] = [10184690, 20519020, 20588413]

# --------------------------------------------------------------------------
# 试验阶段配置
# --------------------------------------------------------------------------
TRIAL_PHASE_CONFIG: dict = {
    "dragon_boat_dates": [date(2026, 6, 19), date(2026, 6, 20), date(2026, 6, 21)],
    "historical_baseline_ranges": [
        (date(2026, 4, 13), date(2026, 4, 26)),
        (date(2026, 5, 11), date(2026, 5, 24)),
    ],
}

# --------------------------------------------------------------------------
# 护栏预警阈值配置（按阶段）
# --------------------------------------------------------------------------
ALERT_THRESHOLDS: dict = {
    "归一化预备期": {
        "active_store_count_wow_yellow": -0.05,
        "active_store_count_wow_red": -0.10,
    },
    "摸底期": {
        "active_store_count_wow_yellow": -0.05,
        "active_store_count_wow_red": -0.10,
        "gmv_deviation_yellow": 0.20,
        "gmv_deviation_red": 0.30,
    },
    "生效期": {
        "order_count_wow_yellow": -0.10,
        "order_count_wow_red": -0.15,
        "active_store_count_wow_yellow": -0.05,
        "active_store_count_wow_red": -0.10,
    },
}

# --------------------------------------------------------------------------
# 各阶段目标抽佣率参考值（phase × region_type × trial_group → target r₀）
# --------------------------------------------------------------------------
TARGET_R0_REFERENCE: dict = {
    "归一化预备期": {
        "自营区域": {
            "对照组": 0.075,
            "试验组一": 0.075,
            "试验组二": 0.075,
            "试验组三": 0.075,
        },
        "代理人区域": {
            "对照组": 0.046,
            "试验组一": 0.046,
            "试验组二": 0.046,
            "试验组三": 0.046,
        },
    },
    "摸底期": {
        "自营区域": {
            "对照组": 0.075,
            "试验组一": 0.075,
            "试验组二": 0.075,
            "试验组三": 0.075,
        },
        "代理人区域": {
            "对照组": 0.046,
            "试验组一": 0.046,
            "试验组二": 0.046,
            "试验组三": 0.046,
        },
    },
    "生效期": {
        "自营区域": {
            "对照组": 0.075,
            "试验组一": 0.095,
            "试验组二": 0.11,
            "试验组三": 0.125,
        },
        "代理人区域": {
            "对照组": 0.046,
            "试验组一": 0.066,
            "试验组二": 0.081,
            "试验组三": 0.096,
        },
    },
}

# --------------------------------------------------------------------------
# 默认 SQLite 输出路径
# --------------------------------------------------------------------------
DEFAULT_DB_PATH = Path(__file__).parent / "data" / "cr_analyze.db"
