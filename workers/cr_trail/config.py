# coding:utf8
"""workers.cr_trail.config -- CR试验配置 专属配置实例

本文件是 CR试验配置 ETL 的唯一「业务知识注入点」，
所有业务字段名、URL、表名均在此定义，供 main.py 和 transformer.py 使用。

lib 层不引用本文件，保证 lib 的通用性。

数据流：
  MaxCompute SQL (conf_goods.sql / conf_area.sql)
    → 提取商品维度 / 区县维度配置信息（最新分区快照）
    → 日期作用域覆盖写入飞书多维表格

设计决策：
  - 无 LARK_SOURCES：数据仅来自 MaxCompute，不需要从飞书拉取
  - 无日期窗口：SQL 使用 MAX_PT 获取最新分区快照，非时间序列
  - 日期作用域清理：运行时由 main.py 根据 SQL 结果的实际日期动态生成清理窗口
"""

from pathlib import Path

from workers.lib import (
    SQLQueryConfig, FieldMapping, LarkTargetConfig, LarkFieldType,
    CleanupCondition, DataRoute,
)

# --------------------------------------------------------------------------
# SQL 文件目录：CR试验 专属 SQL 文件存放位置
# --------------------------------------------------------------------------
SQL_BASE_DIR = Path(__file__).parent / "sql"

# --------------------------------------------------------------------------
# 飞书数据源配置：CR试验商品配置 不需要从飞书拉取数据
# --------------------------------------------------------------------------
LARK_SOURCES: list = []

# --------------------------------------------------------------------------
# MaxCompute SQL 查询配置
# conf_goods.sql 使用 MAX_PT 获取最新分区，不需要日期参数
# --------------------------------------------------------------------------
SQL_QUERIES: list[SQLQueryConfig] = [
    SQLQueryConfig(
        name="conf_goods",
        sql_file="conf_goods.sql",
        depends_on=[],
    ),
    SQLQueryConfig(
        name="conf_area",
        sql_file="conf_area.sql",
        depends_on=[],
    ),
]

# --------------------------------------------------------------------------
# 飞书写入目标配置：conf_商品信息
# URL: https://bggc.feishu.cn/wiki/TcALwGgnciCQQYkPeHYcYf1Cnkd
#       ?table=tblXeBNiHArKWmXm&view=vewTXXVWKv
# --------------------------------------------------------------------------
TARGET_CR_GOODS = LarkTargetConfig(
    name="cr_goods",
    url=(
        "https://bggc.feishu.cn/wiki/TcALwGgnciCQQYkPeHYcYf1Cnkd"
        "?table=tblXeBNiHArKWmXm&view=vewTXXVWKv"
    ),
    table_name="conf_商品信息",
    field_mappings=[
        # -- 基础维度 --
        FieldMapping(source_col="日期", target_field="日期", lark_type=LarkFieldType.DATE),
        FieldMapping(source_col="商城id", target_field="商城id", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="商品id", target_field="商品id", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="商品名称", target_field="商品名称", lark_type=LarkFieldType.TEXT),
        # -- 商品扩展信息（来自 dim_goods_extra_info_daily_full）--
        FieldMapping(source_col="商品等级", target_field="商品等级", lark_type=LarkFieldType.TEXT),
        FieldMapping(source_col="产地", target_field="产地", lark_type=LarkFieldType.TEXT),
        FieldMapping(source_col="包装类型", target_field="包装类型", lark_type=LarkFieldType.TEXT),
        FieldMapping(source_col="单果大小", target_field="单果大小", lark_type=LarkFieldType.TEXT),
        FieldMapping(source_col="色号", target_field="色号", lark_type=LarkFieldType.TEXT),
        FieldMapping(source_col="商品头数", target_field="商品头数", lark_type=LarkFieldType.TEXT),
        # -- 商家信息 --
        FieldMapping(source_col="商家id", target_field="商家id", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="商家名称", target_field="商家名称", lark_type=LarkFieldType.TEXT),
        FieldMapping(source_col="商家类型", target_field="商家类型", lark_type=LarkFieldType.TEXT),
        # -- 类目信息 --
        FieldMapping(source_col="后台类目id", target_field="后台类目id", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="后台类目名称", target_field="后台类目名称", lark_type=LarkFieldType.TEXT),
        # -- 重量 --
        FieldMapping(source_col="净重", target_field="净重", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="毛重", target_field="毛重", lark_type=LarkFieldType.NUMBER),
        # -- 非试验区域定价 --
        FieldMapping(source_col="非试验区域平台销售斤单价", target_field="非试验区域平台销售斤单价", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="非试验区域平台销售件单价", target_field="非试验区域平台销售件单价", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="非试验区域抽佣率", target_field="非试验区域抽佣率", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="非试验区域商家供货斤单价", target_field="非试验区域商家供货斤单价", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="非试验区域商家供货件单价", target_field="非试验区域商家供货件单价", lark_type=LarkFieldType.NUMBER),
        # -- 标记字段 --
        FieldMapping(source_col="是否当日上架", target_field="是否当日上架", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="是否试验周期", target_field="是否试验周期", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="是否试验商品", target_field="是否试验商品", lark_type=LarkFieldType.NUMBER),
    ],
    # 日期作用域清理：运行时由 main.py 根据 SQL 结果的实际日期动态生成清理窗口
    # 重跑时仅删除并替换对应日期的旧数据，不影响其他历史日期
    cleanup_conditions=CleanupCondition.runtime_window(),
)

TARGET_CR_AREA = LarkTargetConfig(
    name="cr_area",
    url=(
        "https://bggc.feishu.cn/wiki/TcALwGgnciCQQYkPeHYcYf1Cnkd"
        "?table=tblgJZiXGW8Zu3fI&view=vewrc0QcYn"
    ),
    table_name="conf_区县信息",
    field_mappings=[
        # -- 基础维度 --
        FieldMapping(source_col="日期", target_field="日期", lark_type=LarkFieldType.DATE),
        FieldMapping(source_col="试验区域id", target_field="试验区域id", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="试验区域名称", target_field="试验区域名称", lark_type=LarkFieldType.TEXT),
        FieldMapping(source_col="修正区域名称", target_field="修正区域名称", lark_type=LarkFieldType.TEXT),
        FieldMapping(source_col="区域别名", target_field="区域别名", lark_type=LarkFieldType.TEXT),
        # -- 行政层级：地址 → 街道 → 区县 → 市 → 省 --
        FieldMapping(source_col="地址id", target_field="地址id", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="地址名称", target_field="地址名称", lark_type=LarkFieldType.TEXT),
        FieldMapping(source_col="街道id", target_field="街道id", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="街道名称", target_field="街道名称", lark_type=LarkFieldType.TEXT),
        FieldMapping(source_col="区县id", target_field="区县id", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="区县名称", target_field="区县名称", lark_type=LarkFieldType.TEXT),
        FieldMapping(source_col="市id", target_field="市id", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="市名称", target_field="市名称", lark_type=LarkFieldType.TEXT),
        FieldMapping(source_col="省id", target_field="省id", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="省名称", target_field="省名称", lark_type=LarkFieldType.TEXT),
        # -- 地理坐标 --
        FieldMapping(source_col="经度", target_field="经度", lark_type=LarkFieldType.TEXT),
        FieldMapping(source_col="纬度", target_field="纬度", lark_type=LarkFieldType.TEXT),
        # -- 区域属性 --
        FieldMapping(source_col="父级区域id", target_field="父级区域id", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="是否有效", target_field="是否有效", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="区域等级", target_field="区域等级", lark_type=LarkFieldType.NUMBER),
        FieldMapping(source_col="试验区域类型", target_field="试验区域类型", lark_type=LarkFieldType.TEXT),
        FieldMapping(source_col="运营类型", target_field="运营类型", lark_type=LarkFieldType.TEXT),
    ],
    # 日期作用域清理：与 conf_goods 一致，运行时根据 SQL 结果的实际日期动态生成清理窗口
    cleanup_conditions=CleanupCondition.runtime_window(),
)

LARK_TARGETS: list[LarkTargetConfig] = [
    TARGET_CR_GOODS,
    TARGET_CR_AREA,
]

# --------------------------------------------------------------------------
# 数据路由配置：SQL 结果直接写入目标表，无需 merge/transform
#
# source_ref = "mc:conf_goods" → SQL 查询结果 DataFrame
# transforms = [] → 无需额外转换，SQL 已产出最终字段
# --------------------------------------------------------------------------
DATA_ROUTES: list[DataRoute] = [
    DataRoute(
        name="cr_goods",
        target=TARGET_CR_GOODS,
        source_ref="mc:conf_goods",
        transforms=[],
        validation_level="warn",
    ),
    DataRoute(
        name="cr_area",
        target=TARGET_CR_AREA,
        source_ref="mc:conf_area",
        transforms=[],
        validation_level="warn",
    ),
]
