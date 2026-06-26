# coding:utf8
"""workers.okr.config -- OKR 专属配置实例

本文件是 OKR 数据处理任务的唯一「业务知识注入点」，
所有业务字段名、URL、表名均在此定义，供 main.py 和 transformer.py 使用。

lib 层不引用本文件，保证 lib 的通用性。
"""

from pathlib import Path

from workers.lib import (
    LarkSourceConfig,
    SQLQueryConfig,
    FieldMapping,
    LarkTargetConfig,
    LarkFieldType,
    CleanupCondition,
    FilterCondition,
    FilterOperator,
    DataRoute,
)

# --------------------------------------------------------------------------
# SQL 文件目录：OKR 专属 SQL 文件存放位置
# --------------------------------------------------------------------------
SQL_BASE_DIR = Path(__file__).parent / "sql"

# --------------------------------------------------------------------------
# 飞书数据源配置：所有需要从飞书多维表格拉取的数据源
# --------------------------------------------------------------------------
LARK_SOURCES: list[LarkSourceConfig] = [
    LarkSourceConfig(
        name="quality_reject_tasks",
        url=(
            "https://bggc.feishu.cn/wiki/IMjSwk5q3ifINkkZ8r2csznQnvp"
            "?table=tblF4i1Ps0aFo8jO&view=vewH8kd9qH"
        ),
        table_name="品控打回商品任务",
        field_names=["日期", "商品id", "任务对象类型", "四级类目名称"],
        date_filter_field="日期",
        date_fields=["日期"],  # 日期类型：去掉时间部分，只保留日期
        lark_extra_start_days=-6,  # 7日重复打回商品数需要向前回溯6天
    ),
    LarkSourceConfig(
        name="ka_cat4_operate",
        url=(
            "https://bggc.feishu.cn/base/KGP2bpMfoagQCzshkisc3luunyb"
            "?table=tblgLUQR2JWnptMB&view=vew1xk1VDI"
        ),
        table_name="数据表",
        field_names=["日期", "店铺ID", "是否下单香蕉", "是否下单麒麟西瓜"],
        date_filter_field="日期",
        date_fields=["日期"],  # 日期类型：去掉时间部分，只保留日期
    ),
]

# --------------------------------------------------------------------------
# MaxCompute SQL 查询配置：所有需要执行的 SQL 查询
# depends_on 声明依赖关系，框架按拓扑顺序执行
# --------------------------------------------------------------------------
SQL_QUERIES: list[SQLQueryConfig] = [
    SQLQueryConfig(
        name="cat1_stat",
        sql_file="cat1_stat_query.sql",
        depends_on=[],
    ),
    SQLQueryConfig(
        name="store_cat4_stat",
        sql_file="store_cat4_query.sql",
        depends_on=[],  # 独立查询，无依赖
        use_temp_table=True,  # 源表缺少 Download 权限，通过临时表 + Table Tunnel 下载
        temp_table_project="datawarehouse_max_dev",  # 临时表创建在 dev project 中
    ),
    SQLQueryConfig(
        name="mall_stat",
        sql_file="mall_stat_query.sql",
        depends_on=[],  # 独立查询，无数据依赖
    ),
    SQLQueryConfig(
        name="cat4_stat",
        sql_file="cat4_stat_query.sql",
        depends_on=[],  # 独立查询，无数据依赖
    ),
    SQLQueryConfig(
        name="cat4_as_type",
        sql_file="cat4_as_type_query.sql",
        depends_on=[],  # 独立查询，无数据依赖
    ),
    SQLQueryConfig(
        name="bd_stat",
        sql_file="bd_stat_query.sql",
        depends_on=[],  # 独立查询，无数据依赖
        use_temp_table=True,  # 与 store_cat4_stat 相同源表，需临时表模式绕过 Download 权限
        temp_table_project="datawarehouse_max_dev",
    ),
]

# --------------------------------------------------------------------------
# 飞书写入目标配置：所有需要写入的飞书多维表格
# 每个目标表提取为命名变量，便于在 LARK_TARGETS 和 DATA_ROUTES 中按名引用
# --------------------------------------------------------------------------
TARGET_OKR_CAT1 = LarkTargetConfig(
    name="okr_cat1",
    url="https://bggc.feishu.cn/wiki/GoetwcGk8ilty7kTKRBcCI48nmc"
    "?table=tbls6KSijm1R6chr&view=vew2WvvDaN",
    table_name="一级类目维度",
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
            source_col="一级类目id",
            target_field="一级类目id",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="一级类目名称",
            target_field="一级类目名称",
            lark_type=LarkFieldType.TEXT,
        ),
        FieldMapping(
            source_col="下单店铺数",
            target_field="下单店铺数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="日活覆盖率",
            target_field="日活覆盖率",
            lark_type=LarkFieldType.PERCENT,
        ),
        FieldMapping(
            source_col="送达金额",
            target_field="送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="剔除特定品类送达金额",
            target_field="剔除特定品类送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="赔付金额",
            target_field="赔付金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="送达数量",
            target_field="送达数量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="售后数量",
            target_field="售后数量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="剔除特定品类赔付金额",
            target_field="剔除特定品类赔付金额",
            lark_type=LarkFieldType.NUMBER,
        ),
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
            source_col="ka运营品类送达金额",
            target_field="ka运营品类送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="ka运营品类赔付金额",
            target_field="ka运营品类赔付金额",
            lark_type=LarkFieldType.NUMBER,
        ),
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
        FieldMapping(
            source_col="特殊运营品类送达金额",
            target_field="特殊运营品类送达金额",
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
            source_col="特殊运营品类明细单量",
            target_field="特殊运营品类明细单量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="特殊运营品类售后数量",
            target_field="特殊运营品类售后数量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="非品质问题赔付金额",
            target_field="非品质问题赔付金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="非品质问题售后数量",
            target_field="非品质问题售后数量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="非品质问题售后单量",
            target_field="非品质问题售后单量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="升级售后总赔付金额",
            target_field="升级售后总赔付金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="升级售后平台赔付金额",
            target_field="升级售后平台赔付金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="升级售后单量",
            target_field="升级售后单量",
            lark_type=LarkFieldType.NUMBER,
        ),
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
    ],
    # 写入前清理条件：运行时由 DateRangeParams 决定精确窗口
    cleanup_conditions=CleanupCondition.runtime_window(),
    # 其他可用选项：
    # 固定窗口:   CleanupCondition.date_window("日期", date(2026,5,1), date(2026,5,31))
    # 多条件组合: CleanupCondition(conditions=[
    #     FilterCondition("日期", FilterOperator.IS_GREATER, [datetime(2026, 5, 1)]),
    #     FilterCondition("商城", FilterOperator.IS, ["标果长沙"]),
    # ])
    # 清空全表: CleanupCondition(conditions=[
    #     FilterCondition("日期", FilterOperator.IS_NOT_EMPTY)
    # ])
    # 不清理: cleanup_conditions=None
)

# 后续新增目标表只需在此定义新的 TARGET_XXX 变量，并加入 LARK_TARGETS 列表

TARGET_OKR_CAT4_AS_TYPE = LarkTargetConfig(
    name="okr_cat4_as_type",
    url=(
        "https://bggc.feishu.cn/wiki/GoetwcGk8ilty7kTKRBcCI48nmc"
        "?table=tbls6KSijm1R6chr&view=vew2WvvDaN"
    ),
    table_name="四级类目售后类型维度",
    field_mappings=[
        FieldMapping(
            source_col="日期", target_field="日期", lark_type=LarkFieldType.DATE
        ),
        FieldMapping(
            source_col="商城id", target_field="商城id", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="一级类目id",
            target_field="一级类目id",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="四级类目id",
            target_field="四级类目id",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="四级类目名称",
            target_field="四级类目名称",
            lark_type=LarkFieldType.TEXT,
        ),
        FieldMapping(
            source_col="售后类型", target_field="售后类型", lark_type=LarkFieldType.TEXT
        ),
        FieldMapping(
            source_col="售后赔付率",
            target_field="售后赔付率",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="售后赔付率排名",
            target_field="售后赔付率排名",
            lark_type=LarkFieldType.NUMBER,
        ),
    ],
    cleanup_conditions=CleanupCondition.runtime_window(),
)

TARGET_OKR_MALL = LarkTargetConfig(
    name="okr_mall",
    url=(
        "https://bggc.feishu.cn/wiki/GoetwcGk8ilty7kTKRBcCI48nmc"
        "?table=tbls6KSijm1R6chr&view=vew2WvvDaN"
    ),
    table_name="商城维度表",
    field_mappings=[
        # -- 日期 --
        FieldMapping(
            source_col="日期", target_field="日期", lark_type=LarkFieldType.DATE
        ),
        # -- mall_stat: 基础维度 --
        FieldMapping(
            source_col="商城id", target_field="商城id", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="商城", target_field="商城", lark_type=LarkFieldType.TEXT
        ),
        # -- mall_stat: 店铺统计 --
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
        # -- mall_stat: 金额 --
        FieldMapping(
            source_col="送达金额",
            target_field="送达金额",
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
            source_col="剔除特定品类送达金额",
            target_field="剔除特定品类送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="剔除特定品类赔付金额",
            target_field="剔除特定品类赔付金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="剔除特定品类售后赔付率",
            target_field="剔除特定品类售后赔付率",
            lark_type=LarkFieldType.NUMBER,
        ),
        # -- mall_stat: SKU --
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
        FieldMapping(
            source_col="ka运营品类送达金额",
            target_field="ka运营品类送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="ka运营品类赔付金额",
            target_field="ka运营品类赔付金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        # -- mall_stat: 四级类目渗透率 --
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
        # -- mall_stat: 流量 --
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
        # -- mall_stat: 规模（蔬菜/水果） --
        FieldMapping(
            source_col="蔬菜在售sku数",
            target_field="蔬菜在售sku数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="蔬菜送达金额",
            target_field="蔬菜送达金额",
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
            source_col="水果送达金额",
            target_field="水果送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="水果送达金额占比",
            target_field="水果货值占比",
            lark_type=LarkFieldType.NUMBER,
        ),
        # -- mall_stat: 特殊品类运营 --
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
        # -- quality_reject_tasks 聚合指标 --
        FieldMapping(
            source_col="打回水果次数",
            target_field="打回水果次数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="打回水果商品数",
            target_field="打回水果商品数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="7日重复打回商品数",
            target_field="7日重复打回商品数",
            lark_type=LarkFieldType.NUMBER,
        ),
        # -- ka_cat4_operate + store_cat4_stat 聚合指标 --
        FieldMapping(
            source_col="直营区域送达金额",
            target_field="直营区域送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="代理人区域送达金额",
            target_field="代理人区域送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="实验区域送达金额",
            target_field="实验区域送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="蔬菜干货直营下单店铺数",
            target_field="蔬菜干货直营下单店铺数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="代理人区域榴莲下单数量",
            target_field="代理人区域榴莲下单数量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="榴莲下单店铺数",
            target_field="榴莲下单店铺数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="试点区域对比送达金额",
            target_field="试点区域对比送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="试点品类售后店铺数",
            target_field="试点品类售后店铺数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="试点品类8日售后复购店铺数",
            target_field="试点品类8日售后复购店铺数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="试点KA品类门店数",
            target_field="试点KA品类门店数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="试点KA品类门店8日复购品类门店数",
            target_field="试点KA品类门店8日复购品类门店数",
            lark_type=LarkFieldType.NUMBER,
        ),
        # -- ka_cat4_operate: 水果抽佣/送达金额 + 抽佣率 --
        FieldMapping(
            source_col="直营区域水果抽佣金额",
            target_field="直营区域水果抽佣金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="代理人区域水果抽佣金额",
            target_field="代理人区域水果抽佣金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="直营区域水果送达金额",
            target_field="直营区域水果送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="代理人区域水果送达金额",
            target_field="代理人区域水果送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="直营区域水果抽佣率",
            target_field="直营区域水果抽佣率",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="代理人区域水果抽佣率",
            target_field="代理人区域水果抽佣率",
            lark_type=LarkFieldType.NUMBER,
        ),
        # -- ka_cat4_operate: 水果抽佣/送达金额 + 抽佣率（剔除特殊运营品类）--
        FieldMapping(
            source_col="直营区域水果抽佣金额【剔除特殊运营品类】",
            target_field="直营区域水果抽佣金额【剔除特殊运营品类】",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="代理人区域水果抽佣金额【剔除特殊运营品类】",
            target_field="代理人区域水果抽佣金额【剔除特殊运营品类】",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="直营区域水果送达金额【剔除特殊运营品类】",
            target_field="直营区域水果送达金额【剔除特殊运营品类】",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="代理人区域水果送达金额【剔除特殊运营品类】",
            target_field="代理人区域水果送达金额【剔除特殊运营品类】",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="直营区域水果抽佣率【剔除特殊运营品类】",
            target_field="直营区域水果抽佣率【剔除特殊运营品类】",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="代理人区域水果抽佣率【剔除特殊运营品类】",
            target_field="代理人区域水果抽佣率【剔除特殊运营品类】",
            lark_type=LarkFieldType.NUMBER,
        ),
    ],
    cleanup_conditions=CleanupCondition.runtime_window(),
)

TARGET_OKR_BD = LarkTargetConfig(
    name="okr_bd",
    url=(
        "https://bggc.feishu.cn/wiki/GoetwcGk8ilty7kTKRBcCI48nmc"
        "?table=tbls6KSijm1R6chr&view=vew2WvvDaN"
    ),
    table_name="BD维度统计",
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
            source_col="送达重量",
            target_field="送达重量",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="剔除特殊运营品类送达金额",
            target_field="剔除特殊运营品类送达金额",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="剔除特殊运营品类升级售后总赔付金额",
            target_field="剔除特殊运营品类升级售后总赔付金额",
            lark_type=LarkFieldType.NUMBER,
        ),
    ],
    cleanup_conditions=CleanupCondition.runtime_window(),
)

TARGET_OKR_CAT4 = LarkTargetConfig(
    name="okr_cat4",
    url=(
        "https://bggc.feishu.cn/wiki/GoetwcGk8ilty7kTKRBcCI48nmc"
        "?table=tbls6KSijm1R6chr&view=vew2WvvDaN"
    ),
    table_name="四级类目维度表",
    field_mappings=[
        FieldMapping(
            source_col="日期", target_field="日期", lark_type=LarkFieldType.DATE
        ),
        FieldMapping(
            source_col="月份", target_field="月份", lark_type=LarkFieldType.TEXT
        ),
        FieldMapping(
            source_col="是否月末", target_field="是否月末", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="商城", target_field="商城", lark_type=LarkFieldType.TEXT
        ),
        FieldMapping(
            source_col="一级类目名称", target_field="一级类目名称", lark_type=LarkFieldType.TEXT
        ),
        FieldMapping(
            source_col="二级类目名称", target_field="二级类目名称", lark_type=LarkFieldType.TEXT
        ),
        FieldMapping(
            source_col="三级类目名称", target_field="三级类目名称", lark_type=LarkFieldType.TEXT
        ),
        FieldMapping(
            source_col="四级类目id", target_field="四级类目id", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="四级类目名称", target_field="四级类目名称", lark_type=LarkFieldType.TEXT
        ),
        FieldMapping(
            source_col="送达金额", target_field="送达金额", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="赔付金额", target_field="赔付金额", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="在售sku数", target_field="在售sku数", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="动销sku数", target_field="动销sku数", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="老客户曝光店铺数",
            target_field="老客户曝光店铺数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="老客户下单店铺数",
            target_field="老客户下单店铺数",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="一级类目渗透率",
            target_field="一级类目渗透率",
            lark_type=LarkFieldType.PERCENT,
        ),
        FieldMapping(
            source_col="明细订单数", target_field="明细订单数", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="达标sku数", target_field="达标sku数", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="送达金额排名", target_field="送达金额排名", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="一级类目渗透率排名",
            target_field="一级类目渗透率排名",
            lark_type=LarkFieldType.NUMBER,
        ),
        FieldMapping(
            source_col="上架商品数量", target_field="上架商品数量", lark_type=LarkFieldType.NUMBER
        ),
        FieldMapping(
            source_col="更新商品卡片数量",
            target_field="更新商品卡片数量",
            lark_type=LarkFieldType.NUMBER,
        ),
    ],
    cleanup_conditions=CleanupCondition.runtime_window(),
)

LARK_TARGETS: list[LarkTargetConfig] = [
    TARGET_OKR_CAT1,
    TARGET_OKR_MALL,
    TARGET_OKR_CAT4,
    TARGET_OKR_CAT4_AS_TYPE,
    TARGET_OKR_BD,
]

# --------------------------------------------------------------------------
# 数据路由配置：定义数据从源到目标的完整流转路径
# 当 DATA_ROUTES 非空时，main.py 使用路由模式写入；
# 当 DATA_ROUTES 为空时，回退到 LARK_TARGETS 模式（向后兼容）
#
# source_ref 引用规则：
#   "result"      → 主管道 merge + transform 后的结果
#   "lark:<name>" → 飞书源提取的 DataFrame
#   "mc:<name>"   → SQL 查询结果的 DataFrame
# --------------------------------------------------------------------------
DATA_ROUTES: list[DataRoute] = [
    DataRoute(
        name="okr_cat1",
        target=TARGET_OKR_CAT1,
        source_ref="mc:cat1_stat",  # cat1_stat 是独立 SQL 结果，不参与 merge
        transforms=[],
        validation_level="warn",
    ),
    DataRoute(
        name="okr_mall",
        target=TARGET_OKR_MALL,
        source_ref="result",
        transforms=[],
        validation_level="warn",
    ),
    DataRoute(
        name="okr_cat4_as_type",
        target=TARGET_OKR_CAT4_AS_TYPE,
        source_ref="mc:cat4_as_type",  # 独立 SQL 结果，不参与 merge
        transforms=[],
        validation_level="warn",
    ),
    DataRoute(
        name="okr_cat4",
        target=TARGET_OKR_CAT4,
        source_ref="mc:cat4_stat",  # 独立 SQL 结果，不参与 merge
        transforms=[],
        validation_level="warn",
    ),
    DataRoute(
        name="okr_bd",
        target=TARGET_OKR_BD,
        source_ref="mc:bd_stat",  # 独立 SQL 结果，不参与 merge
        transforms=[],
        validation_level="warn",
    ),
]
