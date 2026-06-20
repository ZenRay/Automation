# coding:utf8
"""workers.cr_trail_pricing.config -- 试验区域抽佣调价方案生成器 专属配置实例

本文件是 试验区域抽佣调价方案生成器 ETL 的唯一「业务知识注入点」，
所有业务字段名、URL、表名均在此定义，供 main.py 和 transformer.py 使用。

lib 层不引用本文件，保证 lib 的通用性。

数据流：
  飞书多维表格 (6 张配置表)
    → 7 阶段数据处理流水线
    → Excel 调价方案文件

设计决策：
  - 仅从飞书读取数据，不涉及 MaxCompute
  - 输出为 Excel 文件，不写回飞书（无 LarkTargetConfig / DataRoute）
  - 2 张表需日期范围过滤（today 在试验起止日期内），在 pandas 中完成
  - 3 张表使用 API 级别日期过滤（日期 = target_date）
  - 1 张表使用 API 级别日期偏移过滤（conf_商品信息: 日期 = target_date - 1）
"""

from workers.lib import LarkSourceConfig

# --------------------------------------------------------------------------
# 飞书应用标识：所有 6 张配置表所在的飞书多维表格应用
# --------------------------------------------------------------------------
APP_TOKEN = "GmWEbfgCmatLAnsSyaocBisJnoe"

# 所有表共享的 wiki base URL
_WIKI_BASE = "https://bggc.feishu.cn/wiki/TcALwGgnciCQQYkPeHYcYf1Cnkd"

# --------------------------------------------------------------------------
# 飞书数据源配置：6 张配置表
#
# 日期过滤策略（参见 research.md R-001）：
#   - conf_county / conf_trial_goods / conf_hidden_logistics: API 级别精确日期过滤 (日期 = target_date)
#   - conf_goods: API 级别精确日期过滤 (日期 = target_date - 1, 即昨日数据)
#   - conf_trial_group / conf_trial_commission:
#     全量拉取后在 pandas 中按日期范围过滤（需要 "today 在 [start, end] 内" 语义）
# --------------------------------------------------------------------------
LARK_SOURCES: list[LarkSourceConfig] = [
    LarkSourceConfig(
        name="conf_county",
        url=f"{_WIKI_BASE}?table=tblBgJYpBRT18Uvp&view=default",
        table_name="conf_区县信息",
        field_names=[
            "日期",
            "试验区域id",
            "试验区域名称",
            "修正区域名称",
            "区域别名",
            "地址id",
            "地址名称",
            "街道id",
            "街道名称",
            "区县id",
            "区县名称",
            "市id",
            "市名称",
            "省id",
            "省名称",
            "经度",
            "纬度",
            "父级区域id",
            "是否有效",
            "区域等级",
            "试验区域类型",
            "运营类型",
        ],
        date_filter_field="日期",
        date_fields=["日期"],
    ),
    LarkSourceConfig(
        name="conf_goods",
        url=f"{_WIKI_BASE}?table=tblevDYqsTdwu8fo&view=default",
        table_name="conf_商品信息",
        field_names=[
            "日期",
            "商城id",
            "商品id",
            "商品编码",
            "商品名称",
            "后台类目名称",
            "非试验区域抽佣率",
            "毛重",
        ],
        date_filter_field="日期",
        date_fields=["日期"],
        lark_extra_start_days=0,
    ),
    LarkSourceConfig(
        name="conf_trial_group",
        url=f"{_WIKI_BASE}?table=tbl2hCVkpjtMt16J&view=default",
        table_name="conf_试验分组配置",
        field_names=[
            "区域id",
            "区域名称",
            "区域类型",
            "试验分组",
            "试验起始日期",
            "试验结束日期",
        ],
        date_filter_field=None,
        date_fields=["试验起始日期", "试验结束日期"],
    ),
    LarkSourceConfig(
        name="conf_trial_commission",
        url=f"{_WIKI_BASE}?table=tblXeBNiHArKWmXm&view=default",
        table_name="conf_试验周期抽佣率",
        field_names=[
            "试验阶段",
            "运营类型",
            "试验分组",
            "抽佣率",
            "试验起始日期",
            "试验结束日期",
        ],
        date_filter_field=None,
        date_fields=["试验起始日期", "试验结束日期"],
    ),
    LarkSourceConfig(
        name="conf_trial_goods",
        url=f"{_WIKI_BASE}?table=tblQ7tqbptg9Mdrq&view=default",
        table_name="conf_试验商品信息",
        field_names=[
            "商品id",
            "日期",
            "非试验区域抽佣率",
        ],
        date_filter_field="日期",
        date_fields=["日期"],
        lark_extra_start_days=0,
    ),
    LarkSourceConfig(
        name="conf_hidden_logistics",
        url=f"{_WIKI_BASE}?table=tbl7Fcz4jHtbdz9v&view=default",
        table_name="conf_线上隐形物流费",
        field_names=[
            "日期",
            "商城id",
            "市id",
            "市名称",
            "费用类型",
            "费用计算方式",
            "费率",
            "区县费率映射",
        ],
        date_filter_field="日期",
        date_fields=["日期"],
        lark_extra_start_days=0,
    ),
]

# --------------------------------------------------------------------------
# Stage 2: 商品筛选后保留的字段
# --------------------------------------------------------------------------
PRODUCT_KEEP_FIELDS: list[str] = [
    "商品id",
    "商品编码",
    "商品名称",
    "后台类目名称",
    "非试验区域抽佣率",  # 来自 conf_试验商品信息，非 conf_商品信息
    "毛重",  # 来自 conf_商品信息
    "是否试验区域",  # 新增，恒为 1
]

# --------------------------------------------------------------------------
# Stage 4: 抽佣关联后保留的区域字段
# --------------------------------------------------------------------------
REGION_OUTPUT_FIELDS: list[str] = [
    "日期",
    "试验区域id",
    "试验区域名称",
    "市id",
    "省id",
    "区县id",
    "是否试验区域",
    "试验分组",
    "运营类型",
    "抽佣率",
]

# --------------------------------------------------------------------------
# Stage 7: Excel 列名映射（内部名 → 业务名）
# --------------------------------------------------------------------------
COLUMN_RENAME_MAP: dict[str, str] = {
    "试验区域id": "区域id",
    "试验区域名称": "区域名称",
    "商品编码": "商品SKU",
}

# --------------------------------------------------------------------------
# Stage 7: Excel 输出列顺序
# --------------------------------------------------------------------------
OUTPUT_COLUMNS: list[str] = [
    "区域id",
    "区域名称",
    "商品SKU",
    "商品名称",
    "调价方向",
    "调价幅度",
    "设置状态",
    "固定抽佣比例",
    "固定抽佣货值",
    "商品id",
    "后台类目名称",
]
