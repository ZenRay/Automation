# coding:utf8
"""workers.upgrade_after_sale.config -- 订单/售后明细写入配置

需求：
1. 执行两条 SQL（售后明细、订单明细）
2. 写入同一 Base 下两张表（售后明细表、明细订单表）
3. 每次按各自日期窗口清理后写入（幂等）
"""

from pathlib import Path

from workers.lib import (
    CleanupCondition,
    DataRoute,
    FieldMapping,
    LarkFieldType,
    LarkTargetConfig,
    SQLQueryConfig,
)

SQL_BASE_DIR = Path(__file__).parent / "sql"

# 复用同一 Base URL，按 table_name 定位/自动创建表
BASE_URL = (
    "https://bggc.feishu.cn/base/HGDzb2h7MaydFxsqlyAcCpALnB1"
    "?table=tblvkY8n2lcW3I0f&view=vewHlpPmew"
)


def _fm(
    source_col: str, lark_type: LarkFieldType, target_field: str | None = None
) -> FieldMapping:
    return FieldMapping(
        source_col=source_col,
        target_field=target_field or source_col,
        lark_type=lark_type,
    )


# 两条 SQL 独立配置
SQL_QUERIES: list[SQLQueryConfig] = [
    SQLQueryConfig(
        name="after_sale_item",
        sql_file="after_sale_item_query.sql",
        depends_on=[],
    ),
    SQLQueryConfig(
        name="order_item",
        sql_file="order_item_query.sql",
        depends_on=[],
        use_temp_table=True,
        temp_table_project="datawarehouse_max_dev",
    ),
]

# 各 SQL 独立 offset（可在 main 参数中覆盖）
QUERY_WINDOWS = {
    "after_sale_item": {"start": -7, "end": 0},
    "order_item": {"start": -7, "end": 0},
}

# 应用层写入重试（route 粒度）
ROUTE_RETRY_MAX_ATTEMPTS = 3
ROUTE_RETRY_BACKOFF_SECONDS = 0.8
ROUTE_RETRY_BACKOFF_MULTIPLIER = 2.0
RETRYABLE_ERROR_PATTERNS = [
    "timeout",
    "temporar",
    "429",
    "502",
    "503",
    "504",
    "1254290",
    "1254291",
    "1254607",
    "write conflict",
]

# 飞书附件字段降级策略
ENABLE_ATTACHMENT_BAK = True
ATTACHMENT_BAK_SUFFIX = "_bak_raw"
ATTACHMENT_BAK_SOURCE_FIELDS = [
    "客户申请举证视频",
]

# 飞书附件上限按 20MB 处理，运行时阈值保留 1MB 安全边界。
FEISHU_ATTACHMENT_MAX_MB = 20
ATTACHMENT_MAX_SIZE_MB = FEISHU_ATTACHMENT_MAX_MB - 1

TARGET_AFTER_SALE = LarkTargetConfig(
    name="after_sale_detail",
    url=BASE_URL,
    table_name="售后明细表",
    field_mappings=[
        _fm("申请日期", LarkFieldType.DATE),
        _fm("售后单id", LarkFieldType.NUMBER),
        _fm("售后编号", LarkFieldType.TEXT),
        _fm("售后赔付单号", LarkFieldType.TEXT),
        _fm("售后类型", LarkFieldType.TEXT),
        _fm("订单id", LarkFieldType.NUMBER),
        _fm("售后单编号", LarkFieldType.TEXT),
        _fm("明细订单id", LarkFieldType.NUMBER),
        _fm("明细订单编号", LarkFieldType.TEXT),
        _fm("商品id", LarkFieldType.NUMBER),
        _fm("商品编码", LarkFieldType.TEXT),
        _fm("商品名称", LarkFieldType.TEXT),
        _fm("商品不良率", LarkFieldType.TEXT),
        _fm("后台类目id", LarkFieldType.NUMBER),
        _fm("后台类目名称", LarkFieldType.TEXT),
        _fm("等级", LarkFieldType.TEXT),
        _fm("产地", LarkFieldType.TEXT),
        _fm("包装类型", LarkFieldType.TEXT),
        _fm("商家名称", LarkFieldType.TEXT),
        _fm("店铺id", LarkFieldType.NUMBER),
        _fm("省id", LarkFieldType.NUMBER),
        _fm("省名称", LarkFieldType.TEXT),
        _fm("市id", LarkFieldType.NUMBER),
        _fm("市名称", LarkFieldType.TEXT),
        _fm("区县id", LarkFieldType.NUMBER),
        _fm("区县名称", LarkFieldType.TEXT),
        _fm("送达签收照片", LarkFieldType.ATTACHMENT),
        _fm("门店预期赔付金额", LarkFieldType.NUMBER),
        _fm("门店申请重量", LarkFieldType.NUMBER),
        _fm("门店申请数量", LarkFieldType.NUMBER),
        _fm("客户申请问题", LarkFieldType.TEXT),
        _fm("客户申请举证图片", LarkFieldType.ATTACHMENT),
        _fm("客户申请举证视频", LarkFieldType.ATTACHMENT),
        _fm("客服id", LarkFieldType.NUMBER),
        _fm("客服姓名", LarkFieldType.TEXT),
        _fm("判责售后类型", LarkFieldType.TEXT),
        _fm("客户申请售后备注", LarkFieldType.TEXT),
        _fm("客户申请问题类型", LarkFieldType.TEXT),
        _fm("判责后门店接收信息", LarkFieldType.TEXT),
        _fm("判责后后台信息", LarkFieldType.TEXT),
        _fm("客户实收金额", LarkFieldType.NUMBER),
        _fm("售后单状态", LarkFieldType.TEXT),
        _fm("实际售后单状态", LarkFieldType.TEXT),
        _fm("到货时间", LarkFieldType.TEXT),
        _fm("申请时间", LarkFieldType.TEXT),
        _fm("申领时间", LarkFieldType.TEXT),
        _fm("处理时间", LarkFieldType.TEXT),
        _fm("处理时长", LarkFieldType.NUMBER),
        _fm("商城id", LarkFieldType.NUMBER),
        _fm("运营区域类型", LarkFieldType.TEXT),
    ],
    cleanup_conditions=CleanupCondition.runtime_window(),
)

if ENABLE_ATTACHMENT_BAK:
    TARGET_AFTER_SALE.field_mappings.extend(
        [
            _fm(
                f"{source_col}{ATTACHMENT_BAK_SUFFIX}",
                LarkFieldType.TEXT,
            )
            for source_col in ATTACHMENT_BAK_SOURCE_FIELDS
        ]
    )

TARGET_ORDER_ITEM = LarkTargetConfig(
    name="order_detail",
    url=BASE_URL,
    table_name="明细订单表",
    field_mappings=[
        _fm("日期", LarkFieldType.DATE),
        _fm("明细订单id", LarkFieldType.NUMBER),
        _fm("明细订单编号", LarkFieldType.TEXT),
        _fm("订单id", LarkFieldType.NUMBER),
        _fm("订单编号", LarkFieldType.TEXT),
        _fm("商品id", LarkFieldType.NUMBER),
        _fm("商品名称", LarkFieldType.TEXT),
        _fm("商家名称", LarkFieldType.TEXT),
        _fm("后台类目id", LarkFieldType.NUMBER),
        _fm("后台类目名称", LarkFieldType.TEXT),
        _fm("等级", LarkFieldType.TEXT),
        _fm("产地", LarkFieldType.TEXT),
        _fm("包装类型", LarkFieldType.TEXT),
        _fm("店铺id", LarkFieldType.NUMBER),
        _fm("省id", LarkFieldType.NUMBER),
        _fm("省名称", LarkFieldType.TEXT),
        _fm("市id", LarkFieldType.NUMBER),
        _fm("市名称", LarkFieldType.TEXT),
        _fm("区县id", LarkFieldType.NUMBER),
        _fm("区县名称", LarkFieldType.TEXT),
        _fm("网格id", LarkFieldType.NUMBER),
        _fm("网格名称", LarkFieldType.TEXT),
        _fm("下单数量", LarkFieldType.NUMBER),
        _fm("下单金额", LarkFieldType.NUMBER),
        _fm("送货金额", LarkFieldType.NUMBER),
        _fm("送货数量", LarkFieldType.NUMBER),
        _fm("实付金额", LarkFieldType.NUMBER),
        _fm("商城id", LarkFieldType.NUMBER),
        _fm("运营区域类型", LarkFieldType.TEXT),
    ],
    cleanup_conditions=CleanupCondition.runtime_window(),
)

LARK_TARGETS: list[LarkTargetConfig] = [TARGET_AFTER_SALE, TARGET_ORDER_ITEM]

DATA_ROUTES: list[DataRoute] = [
    DataRoute(
        name="after_sale_detail",
        target=TARGET_AFTER_SALE,
        source_ref="mc:after_sale_item",
        transforms=[],
        validation_level="warn",
    ),
    DataRoute(
        name="order_detail",
        target=TARGET_ORDER_ITEM,
        source_ref="mc:order_item",
        transforms=[],
        validation_level="warn",
    ),
]

# 路由清理窗口日期字段
ROUTE_DATE_FIELDS = {
    "after_sale_detail": "申请日期",
    "order_detail": "日期",
}
