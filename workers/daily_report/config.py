# coding:utf8
"""workers.daily_report.config -- 日报（Daily Report）专属配置实例

本文件是日报数据处理任务的唯一「业务知识注入点」，
所有业务字段名、URL、表名均在此定义，供 main.py 使用。

lib 层不引用本文件，保证 lib 的通用性。
"""

from pathlib import Path

from workers.lib import (
    SQLQueryConfig,
    FieldMapping,
    LarkTargetConfig,
    LarkFieldType,
    CleanupCondition,
    DataRoute,
)

# --------------------------------------------------------------------------
# SQL 文件目录：日报专属 SQL 文件存放位置
# --------------------------------------------------------------------------
SQL_BASE_DIR = Path(__file__).parent / "sql"

# --------------------------------------------------------------------------
# 飞书多维表格 URL（与 OKR 模块共用同一个多维表格文档）
# --------------------------------------------------------------------------
LARK_BASE_URL = (
    "https://bggc.feishu.cn/wiki/GoetwcGk8ilty7kTKRBcCI48nmc"
    "?table=tbls6KSijm1R6chr&view=vew2WvvDaN"
)

# --------------------------------------------------------------------------
# MaxCompute SQL 查询配置
# --------------------------------------------------------------------------
SQL_QUERIES: list[SQLQueryConfig] = [
    SQLQueryConfig(
        name="dr_bd_stat",
        sql_file="bd_stat_query.sql",
        depends_on=[],
        use_temp_table=True,  # 源表缺少 Download 权限，通过临时表 + Table Tunnel 下载
        temp_table_project="datawarehouse_max_dev",
    ),
    SQLQueryConfig(
        name="dr_mall_stat",
        sql_file="mall_stat_query.sql",
        depends_on=[],
    ),
]

# --------------------------------------------------------------------------
# 飞书写入目标配置
# --------------------------------------------------------------------------
TARGET_DR_BD = LarkTargetConfig(
    name="dr_bd",
    url=LARK_BASE_URL,
    table_name="drBD维度统计",
    field_mappings=[
        FieldMapping(
            source_col="日期", target_field="日期", lark_type=LarkFieldType.DATE
        ),
        FieldMapping(
            source_col="商城id", target_field="商城id", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="商城", target_field="商城", lark_type=LarkFieldType.TEXT
        ),
        FieldMapping(
            source_col="网格运营类型",
            target_field="网格运营类型",
            lark_type=LarkFieldType.TEXT,
        ),
        FieldMapping(
            source_col="bdid", target_field="bdid", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="bd姓名", target_field="bd姓名", lark_type=LarkFieldType.TEXT
        ),
        FieldMapping(
            source_col="是否负责ka网格",
            target_field="是否负责ka网格",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="是否负责线上",
            target_field="是否负责线上",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="是否负责代理人",
            target_field="是否负责代理人",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="下单店铺数",
            target_field="下单店铺数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="送达金额",
            target_field="送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="水果送达金额",
            target_field="水果送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="送达重量",
            target_field="送达重量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="非榴莲品类送达金额",
            target_field="非榴莲品类送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="非榴莲品类升级售后总赔付金额",
            target_field="非榴莲品类升级售后总赔付金额",
            lark_type=LarkFieldType.NUMBER,
        ),
    ],
    cleanup_conditions=CleanupCondition.runtime_window(),
)

TARGET_DR_MALL = LarkTargetConfig(
    name="dr_mall",
    url=LARK_BASE_URL,
    table_name="dr中心仓维度统计",
    field_mappings=[
        # -- 基础维度 --
        FieldMapping(
            source_col="日期", target_field="日期", lark_type=LarkFieldType.DATE
        ),
        FieldMapping(
            source_col="商城id", target_field="商城id", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="商城", target_field="商城", lark_type=LarkFieldType.TEXT
        ),
        # -- 店铺统计 --
        FieldMapping(
            source_col="下单店铺数",
            target_field="下单店铺数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="老客户下单店铺数",
            target_field="老客户下单店铺数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="新客户下单店铺数",
            target_field="新客户下单店铺数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="流失客户下单店铺数",
            target_field="流失客户下单店铺数",
            lark_type=LarkFieldType.NUMBER,
        ),
        # -- 金额 --
        FieldMapping(
            source_col="送达金额",
            target_field="送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="送达重量",
            target_field="送达重量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="蔬菜送达金额",
            target_field="蔬菜送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="蔬菜送达重量",
            target_field="蔬菜送达重量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="水果送达金额",
            target_field="水果送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="水果送达重量",
            target_field="水果送达重量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="赔付金额",
            target_field="赔付金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="售后赔付率",
            target_field="售后赔付率",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="非榴莲品类送达金额",
            target_field="非榴莲品类送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="非榴莲品类赔付金额",
            target_field="非榴莲品类赔付金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="非榴莲品类售后赔付率",
            target_field="非榴莲品类售后赔付率",
            lark_type=LarkFieldType.NUMBER,
        ),
        # -- SKU --
        FieldMapping(
            source_col="在售sku数",
            target_field="在售sku数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="动销sku数",
            target_field="动销sku数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="动销水果sku数",
            target_field="动销水果sku数",
            lark_type=LarkFieldType.NUMBER,
        ),
        # -- 四级类目渗透率 --
        FieldMapping(
            source_col="在售四级类目数",
            target_field="在售四级类目数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="动销四级类目数",
            target_field="动销四级类目数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="一级类目渗透率超过35点的四级类目数",
            target_field="一级类目渗透率超过35点的四级类目数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="一级类目渗透率【25,35）的四级类目数",
            target_field="一级类目渗透率【25,35）的四级类目数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="一级类目渗透率【15,25）的四级类目数",
            target_field="一级类目渗透率【15,25）的四级类目数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="一级类目渗透率【5,15）的四级类目数",
            target_field="一级类目渗透率【5,15）的四级类目数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="一级类目渗透率低于5点的四级类目数",
            target_field="一级类目渗透率低于5点的四级类目数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="一级类目渗透率超过15点的四级类目数",
            target_field="一级类目渗透率超过15点的四级类目数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="一级类目渗透率超过15点且排名top3的四级类目数",
            target_field="一级类目渗透率超过15点且排名top3的四级类目数",
            lark_type=LarkFieldType.NUMBER,
        ),
        # -- 流量 --
        FieldMapping(
            source_col="曝光店铺数",
            target_field="曝光店铺数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="买过页面曝光店铺数",
            target_field="买过页面曝光店铺数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="买过页面曝光数量",
            target_field="买过页面曝光数量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="买过页面使用率",
            target_field="买过页面使用率",
            lark_type=LarkFieldType.NUMBER,
        ),
        # -- 规模（蔬菜/水果）--
        FieldMapping(
            source_col="蔬菜在售sku数",
            target_field="蔬菜在售sku数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="蔬菜赔付金额",
            target_field="蔬菜赔付金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="蔬菜赔付率",
            target_field="蔬菜赔付率",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="水果送达金额占比",
            target_field="水果送达金额占比",
            lark_type=LarkFieldType.NUMBER,
        ),
        # -- 特殊品类运营 --
        FieldMapping(
            source_col="特殊运营品类送达金额",
            target_field="特殊运营品类送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="特殊运营品类明细单量",
            target_field="特殊运营品类明细单量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="一级类目渗透率超过10点的特殊运营四级类目数",
            target_field="一级类目渗透率超过10点的特殊运营四级类目数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="特殊运营品类商品一级类目渗透率超10点的商品数",
            target_field="特殊运营品类商品一级类目渗透率超10点的商品数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="特殊运营品类售后单数量",
            target_field="特殊运营品类售后单数量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="特殊运营品类非品质问题赔付金额",
            target_field="特殊运营品类非品质问题赔付金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="特殊运营品类非品质问题售后数量",
            target_field="特殊运营品类非品质问题售后数量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="特殊运营品类非品质问题售后单量",
            target_field="特殊运营品类非品质问题售后单量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="特殊运营品类赔付率",
            target_field="特殊运营品类赔付率",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="特殊运营非品质问题售后单占比",
            target_field="特殊运营非品质问题售后单占比",
            lark_type=LarkFieldType.NUMBER,
        ),
    ],
    cleanup_conditions=CleanupCondition.runtime_window(),
)

LARK_TARGETS: list[LarkTargetConfig] = [
    TARGET_DR_BD,
    TARGET_DR_MALL,
]

# --------------------------------------------------------------------------
# 数据路由配置：定义数据从源到目标的完整流转路径
# --------------------------------------------------------------------------
DATA_ROUTES: list[DataRoute] = [
    DataRoute(
        name="dr_bd",
        target=TARGET_DR_BD,
        source_ref="mc:dr_bd_stat",
        transforms=[],
        validation_level="warn",
    ),
    DataRoute(
        name="dr_mall",
        target=TARGET_DR_MALL,
        source_ref="mc:dr_mall_stat",
        transforms=[],
        validation_level="warn",
    ),
]
