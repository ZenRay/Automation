# coding:utf8
"""workers.lib.type_coercer -- 飞书字段类型转换层

飞书多维表格字段读取到的 Python 类型与 MaxCompute SQL 返回的 pandas 类型不一致，
尤其日期字段差异最大（飞书存毫秒时间戳，SQL 返回 datetime64 或字符串）。

本模块提供 FieldTypeCoercer，将 DataFrame 列值按飞书字段 type 转换为
飞书 batch_create API 所需的写入格式。

飞书字段 type 参考：
  1=文本  2=数字  3=单选  4=多选  5=日期  7=复选框
  11=人员  13=电话  15=超链接  17=附件  18=单向关联
  1001=创建时间  1002=最后更新时间  1003=创建人  1004=修改人
"""

import calendar
import logging
from datetime import datetime, date, timezone
from typing import Any

import numpy as np
import pandas as pd

from .models import FieldMapping, LarkFieldType

logger = logging.getLogger("workers.lib.type_coercer")


class FieldTypeCoercer:
    """按飞书字段 type 将 DataFrame 列值转换为飞书写入所需格式

    使用方式：
        coercer = FieldTypeCoercer()
        records = coercer.apply_to_dataframe(df, field_mappings)
        # records 格式: [{"fields": {"字段名": 值, ...}}, ...]
        # 可直接传入 LarkMultiDimTable.add_batch_records()
    """

    # ------------------------------------------------------------------
    # 单值转换
    # ------------------------------------------------------------------

    def coerce_for_write(self, value: Any, lark_type: int, ui_type: str = "") -> Any:
        """将单个单元格值转换为飞书写入格式

        Args:
            value:      原始值（来自 DataFrame 单元格）
            lark_type:  飞书字段类型（LarkFieldType 枚举或 int）
            ui_type:    可选 ui_type，用于精确区分同 type 不同展示类型

        Returns:
            转换后的值，符合飞书 batch_create 要求的格式
        """
        # 空值统一处理
        if self._is_null(value):
            return self._null_for_type(lark_type)

        try:
            if lark_type == LarkFieldType.TEXT:
                return self._coerce_text(value)
            elif lark_type == LarkFieldType.NUMBER:
                return self._coerce_number(value, ui_type)
            elif lark_type == LarkFieldType.SINGLE_SELECT:
                return self._coerce_single_select(value)
            elif lark_type == LarkFieldType.MULTI_SELECT:
                return self._coerce_multi_select(value)
            elif lark_type == LarkFieldType.DATE:
                return self._coerce_date(value)
            elif lark_type == LarkFieldType.CHECKBOX:
                return self._coerce_checkbox(value)
            elif lark_type == LarkFieldType.PERSON:
                return self._coerce_person(value)
            elif lark_type == LarkFieldType.URL:
                return self._coerce_url(value)
            else:
                # 未知类型原样传递，避免数据丢失
                logger.debug(f"Unknown lark_type={lark_type}, passing value as-is: {value!r}")
                return value
        except Exception as e:
            logger.warning(
                f"Type coercion failed: field_type={lark_type}, "
                f"value_type={type(value).__name__}, value={value!r}: {e}. "
                f"Returning raw value."
            )
            return value

    # ------------------------------------------------------------------
    # 各类型转换实现
    # ------------------------------------------------------------------

    def _coerce_text(self, value: Any) -> str:
        """type=1 文本：转为字符串"""
        return str(value)

    def _coerce_number(self, value: Any, ui_type: str = "") -> float:
        """type=2 数字/进度/货币/评分：转为浮点数"""
        return float(value)

    def _coerce_single_select(self, value: Any) -> dict:
        """type=3 单选：转为 {"text": "..."} 格式"""
        return {"text": str(value)}

    def _coerce_multi_select(self, value: Any) -> list[dict]:
        """type=4 多选：转为 [{"text": "..."}, ...] 格式

        支持输入：
          - 逗号分隔的字符串 "a,b,c"
          - 已经是 list 格式（直接返回）
          - 单个值（包装为单元素列表）
        """
        if isinstance(value, list):
            # 已经是列表格式，确保每个元素都是 {"text": ...}
            return [
                {"text": str(v)} if not isinstance(v, dict) else v
                for v in value
            ]
        text = str(value)
        # 按逗号分割
        return [{"text": v.strip()} for v in text.split(",") if v.strip()]

    def _coerce_date(self, value: Any) -> int:
        """type=5 日期：转为飞书所需的毫秒时间戳（int）

        核心规则：统一按 UTC 零点生成时间戳，确保日期语义不受时区影响。
        原因：飞书 ExactDate filter 会将时间戳截断为「文档时区当天零点」，
        发送 UTC 零点可确保无论系统时区还是文档时区如何，截断后日期都正确。

        支持输入类型：
          - int/float：已是毫秒时间戳，直接返回
          - datetime.datetime：取日期部分，转 UTC 零点毫秒时间戳
          - datetime.date：当天 UTC 零点毫秒时间戳
          - str：尝试解析多种日期格式，取日期部分转 UTC 零点
          - pandas.Timestamp / numpy.datetime64：取日期部分转 UTC 零点
        """
        result = self._to_lark_timestamp(value)
        logger.debug(f"Date coercion: {type(value).__name__}({value!r}) -> {result}")
        return result

    def _coerce_checkbox(self, value: Any) -> bool:
        """type=7 复选框：转为布尔值"""
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "是")
        return bool(value)

    def _coerce_person(self, value: Any) -> list[dict]:
        """type=11 人员：转为 [{"id": "..."}] 格式

        注意：人员字段通常无法从 SQL 查询结果直接获得，
        此转换假设输入已是 open_id 字符串或列表。
        """
        if isinstance(value, list):
            return [{"id": str(v)} for v in value]
        return [{"id": str(value)}]

    def _coerce_url(self, value: Any) -> dict:
        """type=15 超链接：转为 {"link": "...", "text": "..."} 格式"""
        if isinstance(value, dict):
            return value
        url = str(value)
        return {"link": url, "text": url}

    # ------------------------------------------------------------------
    # 时间戳转换核心
    # ------------------------------------------------------------------

    def _to_lark_timestamp(self, value: Any) -> int:
        """将各种日期格式转为飞书导入所需的毫秒时间戳

        飞书日期字段存储的是 UTC 毫秒时间戳（int）。
        核心规则：统一按 UTC 零点生成时间戳，确保日期语义不受时区影响。
        注意：飞书 API 写入时要求的是毫秒时间戳，不是秒。
        """
        # 已经是整数（毫秒时间戳）
        if isinstance(value, (int, np.integer)):
            # 判断是秒还是毫秒（秒级时间戳约 10 位，毫秒约 13 位）
            if value > 9999999999:
                return int(value)
            else:
                return int(value * 1000)

        if isinstance(value, (float, np.floating)):
            if value > 9999999999:
                return int(value)
            else:
                return int(value * 1000)

        # pandas Timestamp: 取日期部分，UTC 零点
        if isinstance(value, pd.Timestamp):
            return calendar.timegm(value.date().timetuple()) * 1000

        # numpy datetime64: 取日期部分，UTC 零点
        if isinstance(value, np.datetime64):
            ts = pd.Timestamp(value)
            return calendar.timegm(ts.date().timetuple()) * 1000

        # datetime: 取日期部分，UTC 零点
        if isinstance(value, datetime):
            return calendar.timegm(value.date().timetuple()) * 1000

        # date（无时间部分，直接 UTC 零点）
        if isinstance(value, date):
            return calendar.timegm(value.timetuple()) * 1000

        # 字符串：尝试多种格式解析，取日期部分 UTC 零点
        if isinstance(value, str):
            for fmt in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%d",
            ):
                try:
                    parsed = datetime.strptime(value, fmt)
                    return calendar.timegm(parsed.date().timetuple()) * 1000
                except ValueError:
                    continue
            # 尝试 pandas 解析
            try:
                ts = pd.Timestamp(value)
                if not pd.isna(ts):
                    return calendar.timegm(ts.date().timetuple()) * 1000
            except Exception:
                pass
            raise ValueError(f"Cannot parse date string: {value!r}")

        raise TypeError(f"Unsupported type for date conversion: {type(value).__name__}, value={value!r}")

    # ------------------------------------------------------------------
    # DataFrame 批量转换
    # ------------------------------------------------------------------

    def apply_to_dataframe(
        self,
        df: pd.DataFrame,
        field_mappings: list[FieldMapping]
    ) -> list[dict]:
        """将整个 DataFrame 按 field_mappings 转换为飞书 batch_create 所需的 records 列表

        Args:
            df:             融合后的结果 DataFrame
            field_mappings: 字段映射规则列表

        Returns:
            list[dict]，每个元素格式: {"fields": {"target_field": coerced_value, ...}}
            可直接传入 LarkMultiDimTable.add_batch_records()

        Raises:
            KeyError: 当 DataFrame 中缺少 field_mapping 指定的 source_col
        """
        records = []
        missing_cols = [m.source_col for m in field_mappings if m.source_col not in df.columns]
        if missing_cols:
            logger.error(f"DataFrame missing columns: {missing_cols}. Available: {list(df.columns)}")
            raise KeyError(f"DataFrame missing required columns: {missing_cols}")

        for row_idx, row in df.iterrows():
            fields = {}
            for mapping in field_mappings:
                raw_value = row[mapping.source_col]
                fields[mapping.target_field] = self.coerce_for_write(
                    raw_value,
                    lark_type=mapping.lark_type,
                    ui_type=mapping.lark_ui_type,
                )
            records.append({"fields": fields})

        logger.info(
            f"Converted {len(records)} rows to Lark records format "
            f"with {len(field_mappings)} field mappings"
        )
        return records

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------

    @staticmethod
    def _is_null(value: Any) -> bool:
        """判断值是否为空（None / NaN / NaT / 空字符串）"""
        if value is None:
            return True
        # float NaN
        if isinstance(value, float) and np.isnan(value):
            return True
        # pandas Timestamp / numpy datetime64 NaT
        if isinstance(value, (pd.Timestamp, np.datetime64)) and pd.isna(value):
            return True
        return False

    @staticmethod
    def _null_for_type(lark_type: int) -> Any:
        """各类型的空值表示

        飞书 batch_create 对空值的处理：
        - 文本/数字：不传该字段或传 None
        - 多选：传空列表
        - 日期：传 None
        """
        if lark_type == 4:
            return []  # 多选空值
        return None  # 其他类型不传或传 None
