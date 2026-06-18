# coding:utf8
"""workers.lib.models -- 通用配置数据模型

定义所有 worker 共用的配置 dataclass。
这些 dataclass 本身不含任何业务值，只是数据结构定义。
具体业务实例由各 worker（如 workers.okr.config）负责创建。
"""

import calendar
import re as _re
from dataclasses import dataclass, field
from datetime import datetime, date, timezone
from decimal import Decimal
from enum import Enum, IntEnum
from typing import Any, Callable, Optional

import numpy as np
import pandas as pd


class LarkFieldType(IntEnum):
    """飞书多维表格字段类型枚举

    使用 IntEnum 而非普通 Enum，原因：
    1. 与飞书 API 的 type 编号直接对应，比较时 LarkFieldType.TEXT == 1 为 True
    2. 向后兼容：已有的硬编码数字仍然可以正常工作
    3. type_coercer 中可以直接用 if lark_type == LarkFieldType.TEXT 判断，可读性远优于 lark_type == 1

    参考文档：https://open.feishu.cn/document/uAjLw4CM/ukTMukTMukTM/reference/bitable-v1/app-table-field/guide
    """
    TEXT = 1            # 文本（默认）、条码（ui_type=Barcode）、邮箱（ui_type=Email）
    NUMBER = 2          # 数字（默认）、进度（ui_type=Progress）、货币（ui_type=Currency）、评分（ui_type=Rating）
    PERCENT = 2         # 百分比/进度（NUMBER 的 ui_type 别名，语义等价于 NUMBER）
    SINGLE_SELECT = 3   # 单选
    MULTI_SELECT = 4    # 多选
    DATE = 5            # 日期
    CHECKBOX = 7        # 复选框
    PERSON = 11         # 人员
    PHONE = 13          # 电话号码
    URL = 15            # 超链接
    ATTACHMENT = 17     # 附件
    SINGLE_LINK = 18    # 单向关联
    LOOKUP = 19         # 查找引用
    FORMULA = 20        # 公式
    DUPLEX_LINK = 21    # 双向关联
    LOCATION = 22       # 地理位置
    GROUP = 23          # 群组
    STAGE = 24          # 流程（只读）
    CREATED_TIME = 1001     # 创建时间（只读）
    MODIFIED_TIME = 1002    # 最后更新时间（只读）
    CREATED_USER = 1003     # 创建人（只读）
    MODIFIED_USER = 1004    # 修改人（只读）
    AUTO_NUMBER = 1005      # 自动编号（只读）


class FilterOperator(str, Enum):
    """飞书多维表格筛选运算符

    使用 str, Enum 混合继承，值直接是飞书 API 要求的字符串，
    序列化时无需额外转换（json.dumps(FilterOperator.IS_GREATER) → '"isGreater"'）。

    注意：部分运算符不支持日期字段，详见飞书文档：
    https://open.feishu.cn/document/docs/bitable-v1/app-table-record/record-filter-guide
    """
    IS = "is"                           # 等于（所有字段类型）
    IS_NOT = "isNot"                    # 不等于（日期不支持）
    CONTAINS = "contains"               # 包含（日期不支持）
    DOES_NOT_CONTAIN = "doesNotContain" # 不包含（日期不支持）
    IS_EMPTY = "isEmpty"                # 为空（所有字段类型）
    IS_NOT_EMPTY = "isNotEmpty"         # 不为空（所有字段类型）
    IS_GREATER = "isGreater"            # 大于（数字、日期）
    IS_GREATER_EQUAL = "isGreaterEqual" # 大于等于（日期不支持）
    IS_LESS = "isLess"                  # 小于（数字、日期）
    IS_LESS_EQUAL = "isLessEqual"       # 小于等于（日期不支持）


# 不需要 value 的运算符（isEmpty / isNotEmpty 要求 value=[]）
_EMPTY_OPERATORS = {FilterOperator.IS_EMPTY, FilterOperator.IS_NOT_EMPTY}


def _to_lark_timestamp(value: Any) -> int:
    """将各种日期/时间格式转为飞书所需的毫秒时间戳（int）

    核心规则：日期/时间类型统一按 UTC 零点生成时间戳。
    原因：飞书 ExactDate filter 会将时间戳截断为「文档时区当天零点」，
    发送 UTC 零点可确保无论系统时区还是文档时区如何，截断后日期都正确。

    支持：int/float（秒或毫秒时间戳）、datetime、date、pd.Timestamp、np.datetime64、str
    """
    if isinstance(value, (int, np.integer)):
        return int(value) if value > 9999999999 else int(value * 1000)
    if isinstance(value, (float, np.floating)):
        return int(value) if value > 9999999999 else int(value * 1000)
    if isinstance(value, pd.Timestamp):
        return calendar.timegm(value.date().timetuple()) * 1000
    if isinstance(value, np.datetime64):
        return calendar.timegm(pd.Timestamp(value).date().timetuple()) * 1000
    if isinstance(value, datetime):
        return calendar.timegm(value.date().timetuple()) * 1000
    if isinstance(value, date):
        return calendar.timegm(value.timetuple()) * 1000
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y/%m/%d"):
            try:
                parsed = datetime.strptime(value, fmt)
                return calendar.timegm(parsed.date().timetuple()) * 1000
            except ValueError:
                continue
        ts = pd.Timestamp(value)
        if not pd.isna(ts):
            return calendar.timegm(ts.date().timetuple()) * 1000
        raise ValueError(f"Cannot parse date string: {value!r}")
    raise TypeError(f"Unsupported type for timestamp: {type(value).__name__}, value={value!r}")


def _convert_filter_value(value: Any) -> list[str]:
    """将 Python 原生值转换为飞书 filter API 要求的 value 格式

    转换规则（参考飞书文档 record-filter-guide）：
      - datetime/date/Timestamp → ["ExactDate", "毫秒时间戳字符串"]
      - int/float/Decimal       → ["数字字符串"]
      - bool                    → ["true"] / ["false"]
      - str                     → ["文本内容"]
    """
    if isinstance(value, bool):
        return [str(value).lower()]
    if isinstance(value, (datetime, date, pd.Timestamp, np.datetime64)):
        return ["ExactDate", str(_to_lark_timestamp(value))]
    if isinstance(value, (int, float, Decimal, np.integer, np.floating)):
        return [str(value)]
    return [str(value)]


# --------------------------------------------------------------------------
# 日期范围参数
# --------------------------------------------------------------------------

@dataclass
class DateRangeParams:
    """日期范围参数，驱动 SQL 查询、飞书拉取、目标表清理的日期窗口

    Attributes:
        start_offset:   起始日偏移量（相对基准日期），-7 表示 T-7
        end_offset:     结束日偏移量（相对基准日期），0 表示 T（当天）
        cleanup_buffer: 清理窗口额外回溯天数，确保清理范围 >= 查询范围
                        实际清理天数 = abs(start_offset) + cleanup_buffer
        date_param:     SQL 中的基准日期表达式，默认 CURRENT_DATE()
                        传入具体日期如 '2026-06-06' 时自动包装为 DATE '2026-06-06'
                        也可直接传入任意 SQL DATE 表达式
        reference_date: 管道基准日期（Python date 对象），用于锚定清理窗口。
                        None 时回退到 date.today()（即当天）。
                        当管道回测历史数据时，必须设置此字段，否则清理窗口
                        会锚定在「今天」而非管道基准日，导致误删范围外数据。

    SQL 模板渲染说明：
        SQL 文件中日期条件统一使用三参数形式：
            BETWEEN DATEADD(${date_param}, ${start_offset}, "dd")
                    AND DATEADD(${date_param}, ${end_offset}, "dd")
        保留 DATEADD 结构让人工维护 SQL 时语义清晰、可读性强。
    """
    start_offset: int = -7
    end_offset: int = 0
    cleanup_buffer: int = 0
    date_param: str = "CURRENT_DATE()"
    reference_date: Optional[date] = None

    @property
    def cleanup_days(self) -> int:
        """目标表清理回溯天数"""
        return abs(self.start_offset) + self.cleanup_buffer

    @property
    def lark_days(self) -> int:
        """飞书数据源拉取天数（取绝对值，保证 >= 查询窗口）"""
        return abs(self.start_offset)

    @property
    def cleanup_window(self) -> tuple[date, date]:
        """清理窗口的 (start_date, end_date)，基于 reference_date 计算

        清理窗口 = 管道处理窗口 + cleanup_buffer 向前扩展。
        确保清理范围精确匹配管道写入范围，不多删、不少删。

        Returns:
            tuple[date, date]: (cleanup_start, cleanup_end) 闭区间
        """
        from datetime import timedelta
        ref = self.reference_date if self.reference_date is not None else date.today()
        start = ref + timedelta(days=self.start_offset - self.cleanup_buffer)
        end = ref + timedelta(days=self.end_offset)
        return (start, end)

    def sql_params(self) -> dict[str, str]:
        """生成 SQL 模板三参数，供 mc_extractor 渲染

        date_param 自动转换规则：
            - CURRENT_DATE()          → 原样透传（已是合法 DATE 表达式）
            - '2026-06-06'            → DATE '2026-06-06'（自动包装 DATE 字面量）
            - DATE '2026-06-06'       → 原样透传（已是 DATE 字面量）
            - 其他含 DATE/CURRENT 的  → 原样透传

        Returns:
            dict:
                date_param:    基准日期表达式
                start_offset:  起始偏移量字符串
                end_offset:    结束偏移量字符串
        """
        dp = self._normalize_date_param(self.date_param)
        return {
            "date_param":   dp,
            "start_offset": str(self.start_offset),
            "end_offset":   str(self.end_offset),
        }

    @staticmethod
    def _normalize_date_param(value: str) -> str:
        """将裸日期字符串自动包装为 MaxCompute DATE 字面量

        MaxCompute DATEADD 第一参数要求 DATE/DATETIME 类型，不接受 STRING。
        当传入 '2026-06-06' 这样的裸日期时，自动包装为 DATE '2026-06-06'。
        """
        stripped = value.strip()
        # 已是函数调用或 DATE 字面量 → 原样返回
        if "(" in stripped or stripped.upper().startswith("DATE"):
            return stripped
        # 匹配 YYYY-MM-DD 格式的裸日期字符串
        if _re.match(r"^\d{4}-\d{2}-\d{2}$", stripped):
            return f"DATE '{stripped}'"
        # 其他情况原样返回（用户可能传入复杂表达式）
        return stripped


@dataclass
class FilterCondition:
    """单条筛选条件

    Attributes:
        field_name: 飞书字段名
        operator:   筛选运算符（FilterOperator 枚举）
        value:      条件值列表，接受 Python 原生类型。
                    空值运算符（isEmpty/isNotEmpty）时传 [] 或省略。
                    示例: [datetime(2026, 5, 1)]、["标果长沙"]、[100.0]
    """
    field_name: str
    operator: FilterOperator
    value: list[Any] = field(default_factory=list)


@dataclass
class CleanupCondition:
    """写入前清理条件

    支持多条件组合筛选，自动将 Python 值转换为飞书 API 要求的格式。

    使用方式：
        # 快捷方式：删除最近 30 天的数据
        CleanupCondition.recent_days("日期", 30)

        # 多条件组合
        CleanupCondition(
            conditions=[
                FilterCondition("日期", FilterOperator.IS_GREATER, [datetime(2026, 5, 1)]),
                FilterCondition("商城", FilterOperator.IS, ["标果长沙"]),
            ]
        )

        # 清空全表
        CleanupCondition(
            conditions=[FilterCondition("日期", FilterOperator.IS_NOT_EMPTY)]
        )

    Attributes:
        conditions:  筛选条件列表
        conjunction: 条件间逻辑关系，"and"（默认）或 "or"
    """
    conditions: list[FilterCondition]
    conjunction: str = "and"
    _is_runtime: bool = field(default=False, repr=False, compare=False)

    @property
    def is_runtime(self) -> bool:
        """是否为运行时窗口占位符（由管道 DateRangeParams 替换）"""
        return self._is_runtime

    @classmethod
    def runtime_window(cls) -> "CleanupCondition":
        """占位哨兵：标记「需要清理，窗口由管道运行时 DateRangeParams 决定」

        在 config.py 中使用此方法代替硬编码的 recent_days / date_window，
        运行时由 _apply_date_range_to_routes 替换为精确的 date_window。

        三种 cleanup_conditions 语义：
          - None                   : 该目标不需要清理
          - CleanupCondition.runtime_window() : 需要清理，窗口由运行时决定（占位）
          - 其他具体条件            : 使用此处定义的固定条件，运行时不覆盖
        """
        return cls(conditions=[], _is_runtime=True)

    @classmethod
    def recent_days(
        cls,
        date_field: str,
        days: int,
        reference_date: Optional[date] = None,
    ) -> "CleanupCondition":
        """快捷构造：删除指定日期字段最近 N 天的记录

        注意：此方法生成单边条件（仅下界），会删除 cutoff 之后的所有记录。
        如需精确控制清理范围（双边窗口），请使用 date_window() 方法。

        Args:
            date_field:     日期字段名
            days:           回溯天数
            reference_date: 基准日期，None 时使用 date.today()。
                            管道回测历史数据时应传入管道基准日，
                            避免清理窗口锚定在「今天」导致误删。
        """
        from datetime import timedelta
        anchor = reference_date if reference_date is not None else date.today()
        cutoff = datetime.combine(anchor, datetime.min.time()) - timedelta(days=days)
        return cls(
            conditions=[
                FilterCondition(date_field, FilterOperator.IS_GREATER, [cutoff])
            ]
        )

    @classmethod
    def date_window(
        cls,
        date_field: str,
        start_date: date,
        end_date: date,
    ) -> "CleanupCondition":
        """快捷构造：删除指定日期窗口 [start_date, end_date] 内的记录

        生成双边条件：IS_GREATER start AND IS_LESS (end+1)，
        精确匹配闭区间 [start_date, end_date]。

        设计说明：
            飞书 ExactDate 的 IS_GREATER 语义实测为 >=（非严格大于），
            因此直接使用 IS_GREATER start_date 即可包含 start_date。
            IS_LESS 语义为严格小于（<），需用 end+1 来表达 <= end。

        Args:
            date_field: 日期字段名
            start_date: 窗口起始日期（含）
            end_date:   窗口结束日期（含）
        """
        from datetime import timedelta
        # IS_GREATER start_date → 实测语义为 >= start_date（包含边界）
        lower = datetime.combine(start_date, datetime.min.time())
        # IS_LESS (end_date + 1day) → 严格小于，等价于 <= end_date
        upper = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
        return cls(conditions=[
            FilterCondition(date_field, FilterOperator.IS_GREATER, [lower]),
            FilterCondition(date_field, FilterOperator.IS_LESS, [upper]),
        ])

    def to_lark_filter(self) -> dict:
        """转换为飞书 search API 的 filter 参数格式

        Returns:
            dict，格式如下：
            {
                "conjunction": "and",
                "conditions": [
                    {"field_name": "日期", "operator": "isGreater",
                     "value": ["ExactDate", "1746057600000"]},
                    ...
                ]
            }

        Raises:
            RuntimeError: 如果哨兵未被替换就直接调用（防止全表删除）
        """
        if self._is_runtime:
            raise RuntimeError(
                "CleanupCondition.runtime_window() 哨兵未被 _apply_date_range_to_routes 替换！"
                "直接执行会导致空 filter 匹配全表并删除所有数据。"
                "请确保通过 run_okr_pipeline() 调用，而非直接使用 config 中的原始 target。"
            )
        if not self.conditions:
            raise RuntimeError(
                "CleanupCondition.conditions 为空！"
                "空 filter 可能匹配全表导致误删，请检查配置。"
            )
        api_conditions = []
        for cond in self.conditions:
            # 空值运算符（isEmpty/isNotEmpty）value 传空列表
            if cond.operator in _EMPTY_OPERATORS or not cond.value:
                api_value = []
            else:
                # 将每个 value 元素转换为飞书 API 格式
                # 对于非空值运算符，取第一个元素作为条件值
                api_value = _convert_filter_value(cond.value[0])

            api_conditions.append({
                "field_name": cond.field_name,
                "operator": cond.operator.value,
                "value": api_value,
            })

        return {
            "conjunction": self.conjunction,
            "conditions": api_conditions,
        }


@dataclass
class LarkSourceConfig:
    """飞书多维表格数据源配置

    Attributes:
        name:                   逻辑名，用于在 transformer 中引用、日志标识
        url:                    飞书多维表格 URL（支持 wiki/base 类型）
        table_name:             目标表名（中文，用于 extract_table_information 匹配）
        view_id:                可选视图 ID，用于过滤特定视图的数据
        field_names:            限定拉取的字段名列表，None 表示拉取所有字段
                                示例: ["商品ID", "商品名称", "打回日期"]
        date_filter_field:      日期筛选字段名，None 表示不做日期筛选
        date_fields:            日期类型字段列表，预处理时去掉时间部分只保留日期
                                示例: ["日期", "打回日期"]
        datetime_fields:        日期时间类型字段列表，预处理时保留时间但统一格式化
                                示例: ["创建时间"]
        datetime_format:        datetime_fields 的输出格式，默认 "%Y-%m-%d %H:%M:%S"
        lark_extra_start_days:  在 SQL 窗口起点基础上的额外偏移（有符号整数）。
                                负值 = 向前回溯（实际拉取起点更早）；正值 = 向后延伸。
                                例：start_offset=-7 且 lark_extra_start_days=-3，
                                则实际拉取 T-10 ~ T（而非 T-7 ~ T）。
                                适用场景：源数据需要比 SQL 查询窗口更长的历史回溯。
        lark_extra_end_days:    在 SQL 窗口终点基础上的额外偏移（有符号整数）。
                                正值 = 向后延伸（实际拉取终点更晚）；负值 = 向前收缩。
                                例：end_offset=0 且 lark_extra_end_days=3，
                                则实际拉取到 T+3（而非 T）。
                                适用场景：源数据需要覆盖滞后更新的时间段。
        date_filter_end_date:   运行时注入的日期筛选上界（由 _apply_date_range_to_lark_sources
                                根据 lark_extra_end_days 自动设置，None=不限上界）。
                                不应在 config.py 中硬编码此字段。
        date_filter_start_date: 运行时注入的日期筛选下界（由 _apply_date_range_to_lark_sources
                                根据 reference_date + start_offset + lark_extra_start_days 自动设置）。
                                不应在 config.py 中硬编码此字段。
    """
    name: str
    url: str
    table_name: str
    view_id: Optional[str] = None
    field_names: Optional[list[str]] = None
    date_filter_field: Optional[str] = None
    date_fields: Optional[list[str]] = None
    datetime_fields: Optional[list[str]] = None
    datetime_format: str = "%Y-%m-%d %H:%M:%S"
    lark_extra_start_days: int = 0
    lark_extra_end_days: int = 0
    date_filter_end_date: Optional[date] = None
    date_filter_start_date: Optional[date] = None


@dataclass
class SQLQueryConfig:
    """MaxCompute SQL 查询配置

    Attributes:
        name:               查询结果的逻辑名，在 transformer 中按此名引用
        sql_file:           SQL 文件路径（相对调用方传入的 sql_base_dir）
        depends_on:         依赖的前置查询名列表，框架按拓扑顺序执行
        use_temp_table:     启用临时表模式（解决 Instance Tunnel 权限不足问题）
                            True 时：SQL 结果先写入临时表，再通过 Table Tunnel 下载
                            False 时：直接通过 Instance Tunnel 下载（默认）
        temp_table_project: 临时表所在 project（如 dev 环境），None 时使用当前 project
        temp_table_schema:  临时表所在 schema（可选），None 时使用默认 schema

    临时表模式说明：
        当 RAM 用户缺少源表的 odps:Download 权限时，Instance Tunnel 无法直接下载
        查询结果。此时可启用 use_temp_table=True，流程变为：
          1. CREATE TABLE temp_table AS <original SQL>
          2. 通过 Table Tunnel 从临时表下载数据（用户对自己创建的表有完整权限）
          3. DROP TABLE 清理临时表
        临时表命名自动生成为 _tmp_{query_name}_{timestamp}，确保唯一性。
    """
    name: str
    sql_file: str
    depends_on: list[str] = field(default_factory=list)
    use_temp_table: bool = False
    temp_table_project: Optional[str] = None
    temp_table_schema: Optional[str] = None


@dataclass
class FieldMapping:
    """DataFrame 列到飞书字段的映射规则

    Attributes:
        source_col:     融合后 DataFrame 中的列名
        target_field:   飞书目标字段名（写入时使用的字段名）
        lark_type:      飞书字段类型，使用 LarkFieldType 枚举（也接受 int，向后兼容）
        lark_ui_type:   可选 ui_type，用于精确转换（如 DateTime vs Date）
    """
    source_col: str
    target_field: str
    lark_type: int  # LarkFieldType 或 int，IntEnum 保证两者可互操作
    lark_ui_type: str = ""


@dataclass
class LarkTargetConfig:
    """飞书多维表格写入目标配置

    Attributes:
        name:                   逻辑名，用于日志标识
        url:                    目标多维表格 URL
        table_name:             目标表名（中文）
        field_mappings:         字段映射规则列表
        cleanup_conditions:     写入前清理旧数据的条件，None 表示不清理
                                示例: CleanupCondition.recent_days("日期", 30)
    """
    name: str
    url: str
    table_name: str
    field_mappings: list[FieldMapping]
    cleanup_conditions: Optional[CleanupCondition] = None


@dataclass
class DataRoute:
    """数据路由配置：定义数据从源到目标的完整流转路径

    与 LarkTargetConfig 的关系：
    - LarkTargetConfig 只描述"目标表长什么样"（URL、字段映射、清理条件）
    - DataRoute 在此基础上增加"数据从哪来、怎么转换"

    source_ref 数据池引用规则：
    - "result"          → 主管道 result_df（merge + transform 后的结果）
    - "lark:<name>"     → 飞书源提取的 DataFrame
    - "mc:<name>"       → SQL 查询结果的 DataFrame
    - 以上引用在 DataRouter 执行时从统一数据池中查找

    向后兼容：
    - 如果 DATA_ROUTES 为空但 LARK_TARGETS 非空，
      main.py 自动将每个 LarkTargetConfig 包装为 DataRoute(target=..., source_ref="result")

    Attributes:
        name:               路由名称，用于日志标识
        target:             目标表配置（复用 LarkTargetConfig）
        source_ref:         数据源引用字符串，格式为 "result" | "lark:<name>" | "mc:<name>"
        transforms:         per-route 转换函数列表，签名 (df: DataFrame) -> DataFrame
                            每个路由独立执行自己的转换，互不影响
        validation_level:   校验级别
                            "strict": 校验失败阻断写入
                            "warn":   校验失败仅警告，继续写入
                            "skip":   跳过校验
    """
    name: str
    target: LarkTargetConfig
    source_ref: str = "result"
    transforms: list[Callable] = field(default_factory=list)
    validation_level: str = "warn"
