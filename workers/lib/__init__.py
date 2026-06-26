# coding:utf8
"""workers.lib - 通用功能层

不含任何业务字段名或业务表名，可被所有 worker 复用。
依赖方向：lib -> automation（现有客户端），lib 不回引任何 worker。
"""

from .models import (
    LarkSourceConfig,
    SQLQueryConfig,
    FieldMapping,
    LarkTargetConfig,
    LarkFieldType,
    FilterOperator,
    FilterCondition,
    CleanupCondition,
    DataRoute,
    DateRangeParams,
    PersistenceConfig,
)
from .type_coercer import FieldTypeCoercer
from .attachment_resolver import AttachmentResolver
from .lark_extractor import extract_all_lark_sources, extract_single_source
from .mc_extractor import execute_all_queries
from .transformer import DataTransformer
from .lark_loader import write_to_all_targets, cleanup_target_table
from .validator import SchemaValidator, ValidationReport, ValidationIssue
from .router import DataRouter

__all__ = [
    # 配置数据模型
    "LarkSourceConfig",
    "SQLQueryConfig",
    "FieldMapping",
    "LarkTargetConfig",
    "DataRoute",
    "DateRangeParams",
    "PersistenceConfig",
    # 飞书字段类型枚举
    "LarkFieldType",
    # 筛选条件
    "FilterOperator",
    "FilterCondition",
    "CleanupCondition",
    # 类型转换
    "FieldTypeCoercer",
    "AttachmentResolver",
    # 数据拉取
    "extract_all_lark_sources",
    "extract_single_source",
    # SQL 执行
    "execute_all_queries",
    # 数据融合管道
    "DataTransformer",
    # 数据写入
    "write_to_all_targets",
    "cleanup_target_table",
    # 写入前校验
    "SchemaValidator",
    "ValidationReport",
    "ValidationIssue",
    # 数据路由
    "DataRouter",
]
