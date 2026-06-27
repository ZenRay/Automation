# coding:utf8
"""workers.lib.validator -- 写入前数据校验层

在 transform 之后、coerce 之前执行校验，此时数据仍是 pandas 原生类型
（datetime64、int64、object 等），校验的是"列的 dtype 和值域是否与
FieldMapping.lark_type 兼容"。

设计理念：
- 校验分为三级：CRITICAL（阻断写入）、WARNING（建议修复）、INFO（信息记录）
- 校验在 coerce 之前的原因：此时数据还是人类可理解的 pandas 类型，
  调试时可以直接看到 "日期列包含字符串 '2026/6/3'" 这样的信息，
  而非已经转换后的毫秒时间戳（如 1717372800000）
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Any, Optional

import numpy as np
import pandas as pd

from .models import FieldMapping, LarkFieldType

logger = logging.getLogger("workers.lib.validator")


# --------------------------------------------------------------------------
# 校验结果数据模型
# --------------------------------------------------------------------------


@dataclass
class ValidationIssue:
    """单条校验问题

    Attributes:
        level:   严重级别 "CRITICAL" | "WARNING" | "INFO"
        field:   字段名（source_col）
        message: 问题描述
        sample:  问题值样例（可选，便于调试）
    """

    level: str
    field: str
    message: str
    sample: Any = None


@dataclass
class ValidationReport:
    """校验报告汇总

    Attributes:
        target_name: 目标表名称
        issues:      校验问题列表
        total_rows:  校验的 DataFrame 总行数
    """

    target_name: str
    issues: list[ValidationIssue] = field(default_factory=list)
    total_rows: int = 0

    @property
    def has_critical(self) -> bool:
        """是否存在 CRITICAL 级别问题"""
        return any(i.level == "CRITICAL" for i in self.issues)

    @property
    def summary(self) -> str:
        """格式化摘要，用于日志输出"""
        critical = sum(1 for i in self.issues if i.level == "CRITICAL")
        warning = sum(1 for i in self.issues if i.level == "WARNING")
        info = sum(1 for i in self.issues if i.level == "INFO")
        return (
            f"Validation [{self.target_name}]: {self.total_rows} rows, "
            f"{critical} critical, {warning} warnings, {info} info"
        )

    def format_details(self, min_level: str = "WARNING") -> str:
        """格式化详细问题列表（用于 debug 日志）

        Args:
            min_level: 最低显示级别，"INFO" 显示所有，"WARNING" 跳过 INFO
        """
        level_order = {"INFO": 0, "WARNING": 1, "CRITICAL": 2}
        threshold = level_order.get(min_level, 1)
        filtered = [i for i in self.issues if level_order.get(i.level, 0) >= threshold]
        if not filtered:
            return ""
        lines = []
        for issue in filtered:
            sample_str = (
                f" (sample: {issue.sample!r})" if issue.sample is not None else ""
            )
            lines.append(
                f"  [{issue.level}] {issue.field}: {issue.message}{sample_str}"
            )
        return "\n".join(lines)


# --------------------------------------------------------------------------
# 校验器
# --------------------------------------------------------------------------


class SchemaValidator:
    """写入前数据校验器

    使用方式：
        validator = SchemaValidator()
        report = validator.validate(df, field_mappings, target_name="okr_cat1")
        if report.has_critical:
            logger.error(report.format_details())
            raise ValueError("Validation failed")
    """

    # NaN 比例警告阈值
    NAN_RATIO_THRESHOLD = 0.5

    def validate(
        self,
        df: pd.DataFrame,
        field_mappings: list[FieldMapping],
        *,
        target_name: str = "",
    ) -> ValidationReport:
        """执行全量校验，返回报告

        Args:
            df:             待校验的 DataFrame（transform 之后、coerce 之前）
            field_mappings: 字段映射规则列表
            target_name:    目标表名称（用于日志）

        Returns:
            ValidationReport：校验报告
        """
        issues: list[ValidationIssue] = []

        # 1. 列存在性检查（CRITICAL）
        issues.extend(self._check_column_existence(df, field_mappings))

        # 以下检查仅对存在的列执行
        existing_mappings = [m for m in field_mappings if m.source_col in df.columns]

        # 2. 日期列检查
        issues.extend(self._check_date_columns(df, existing_mappings))

        # 3. 数值列检查
        issues.extend(self._check_numeric_columns(df, existing_mappings))

        # 4. 文本列检查
        issues.extend(self._check_text_columns(df, existing_mappings))

        # 5. 复选框列检查
        issues.extend(self._check_checkbox_columns(df, existing_mappings))

        # 6. 单选列检查
        issues.extend(self._check_select_columns(df, existing_mappings))

        report = ValidationReport(
            target_name=target_name,
            issues=issues,
            total_rows=len(df),
        )

        # 输出详细日志
        details = report.format_details(min_level="WARNING")
        if details:
            logger.warning(f"Validation details for '{target_name}':\n{details}")

        return report

    # ------------------------------------------------------------------
    # 各类型校验方法
    # ------------------------------------------------------------------

    def _check_column_existence(
        self,
        df: pd.DataFrame,
        field_mappings: list[FieldMapping],
    ) -> list[ValidationIssue]:
        """检查所有 source_col 是否存在于 DataFrame 中"""
        issues = []
        missing = [m for m in field_mappings if m.source_col not in df.columns]
        for m in missing:
            issues.append(
                ValidationIssue(
                    level="CRITICAL",
                    field=m.source_col,
                    message=(
                        f"Column '{m.source_col}' not found in DataFrame. "
                        f"Available columns: {list(df.columns)}"
                    ),
                )
            )
        if missing:
            logger.error(
                f"Missing columns: {[m.source_col for m in missing]}. "
                f"Available: {list(df.columns)}"
            )
        return issues

    def _check_date_columns(
        self,
        df: pd.DataFrame,
        field_mappings: list[FieldMapping],
    ) -> list[ValidationIssue]:
        """检查日期列的兼容性"""
        issues = []
        date_types = {
            LarkFieldType.DATE,
            LarkFieldType.CREATED_TIME,
            LarkFieldType.MODIFIED_TIME,
        }

        for m in field_mappings:
            if m.lark_type not in date_types:
                continue

            col = df[m.source_col]
            non_null = col.dropna()
            if non_null.empty:
                issues.append(
                    ValidationIssue(
                        level="WARNING",
                        field=m.source_col,
                        message="Date column is entirely null/NaN",
                    )
                )
                continue

            # 检查 dtype 分布
            dtype_kinds = set()
            unparseable = []
            sample_size = min(20, len(non_null))
            sample = non_null.head(sample_size)

            for val in sample:
                if isinstance(val, (pd.Timestamp, np.datetime64, datetime, date)):
                    dtype_kinds.add("datetime")
                elif isinstance(val, (int, float, np.integer, np.floating)):
                    dtype_kinds.add("numeric_timestamp")
                elif isinstance(val, str):
                    dtype_kinds.add("string")
                    # 尝试解析
                    try:
                        pd.Timestamp(val)
                    except (ValueError, TypeError):
                        unparseable.append(val)
                else:
                    dtype_kinds.add(f"unknown({type(val).__name__})")
                    unparseable.append(val)

            # INFO: 记录格式分布
            issues.append(
                ValidationIssue(
                    level="INFO",
                    field=m.source_col,
                    message=f"Date column dtype kinds: {dtype_kinds}",
                )
            )

            # CRITICAL: 存在无法解析的值
            if unparseable:
                issues.append(
                    ValidationIssue(
                        level="CRITICAL",
                        field=m.source_col,
                        message=f"Found {len(unparseable)} unparseable date value(s) in sample",
                        sample=unparseable[0],
                    )
                )

        return issues

    def _check_numeric_columns(
        self,
        df: pd.DataFrame,
        field_mappings: list[FieldMapping],
    ) -> list[ValidationIssue]:
        """检查数值列的兼容性"""
        issues = []
        numeric_types = {LarkFieldType.NUMBER, LarkFieldType.PERCENT}

        for m in field_mappings:
            if m.lark_type not in numeric_types:
                continue

            col = df[m.source_col]

            # 检查是否为数值类型
            if not pd.api.types.is_numeric_dtype(col):
                # 尝试转换，看是否有非数值
                converted = pd.to_numeric(col, errors="coerce")
                non_null_original = col.dropna()
                non_null_converted = converted.dropna()
                lost = len(non_null_original) - len(non_null_converted)
                if lost > 0:
                    issues.append(
                        ValidationIssue(
                            level="WARNING",
                            field=m.source_col,
                            message=(
                                f"Column dtype is '{col.dtype}' (not numeric). "
                                f"{lost} non-null values cannot be converted to number."
                            ),
                        )
                    )

            # 检查 NaN 比例
            if len(col) > 0:
                nan_ratio = col.isna().sum() / len(col)
                if nan_ratio > self.NAN_RATIO_THRESHOLD:
                    issues.append(
                        ValidationIssue(
                            level="WARNING",
                            field=m.source_col,
                            message=f"NaN ratio is {nan_ratio:.1%} (threshold: {self.NAN_RATIO_THRESHOLD:.0%})",
                        )
                    )

            # PERCENT 字段值域检查
            is_percent_field = (
                m.lark_type == LarkFieldType.PERCENT
                and (m.lark_ui_type or "").lower() in {"progress", "percent", "percentage"}
            )
            if is_percent_field and pd.api.types.is_numeric_dtype(
                col
            ):
                non_null = col.dropna()
                if not non_null.empty:
                    out_of_range = non_null[(non_null < 0) | (non_null > 1)]
                    if len(out_of_range) > 0:
                        issues.append(
                            ValidationIssue(
                                level="WARNING",
                                field=m.source_col,
                                message=(
                                    f"PERCENT field has {len(out_of_range)} values outside [0, 1]. "
                                    f"Lark progress fields require 0-1 range."
                                ),
                                sample=out_of_range.iloc[0],
                            )
                        )

        return issues

    def _check_text_columns(
        self,
        df: pd.DataFrame,
        field_mappings: list[FieldMapping],
    ) -> list[ValidationIssue]:
        """检查文本列的长度分布"""
        issues = []

        for m in field_mappings:
            if m.lark_type != LarkFieldType.TEXT:
                continue

            col = df[m.source_col]
            non_null = col.dropna()
            if non_null.empty:
                continue

            # 转为字符串后计算长度
            str_lengths = non_null.astype(str).str.len()
            max_len = str_lengths.max()
            mean_len = str_lengths.mean()

            issues.append(
                ValidationIssue(
                    level="INFO",
                    field=m.source_col,
                    message=f"Text column length: max={max_len}, mean={mean_len:.1f}, rows={len(non_null)}",
                )
            )

        return issues

    def _check_checkbox_columns(
        self,
        df: pd.DataFrame,
        field_mappings: list[FieldMapping],
    ) -> list[ValidationIssue]:
        """检查复选框列的值域"""
        issues = []

        for m in field_mappings:
            if m.lark_type != LarkFieldType.CHECKBOX:
                continue

            col = df[m.source_col]
            non_null = col.dropna()
            if non_null.empty:
                continue

            # 检查值域是否合法
            valid_values = {
                True,
                False,
                0,
                1,
                "true",
                "false",
                "True",
                "False",
                "1",
                "0",
                "yes",
                "no",
                "是",
                "否",
            }
            invalid = []
            sample_size = min(50, len(non_null))
            for val in non_null.head(sample_size):
                if val not in valid_values and not isinstance(val, (bool, np.bool_)):
                    invalid.append(val)

            if invalid:
                issues.append(
                    ValidationIssue(
                        level="WARNING",
                        field=m.source_col,
                        message=f"Found {len(invalid)} values not in expected checkbox domain",
                        sample=invalid[0],
                    )
                )

        return issues

    def _check_select_columns(
        self,
        df: pd.DataFrame,
        field_mappings: list[FieldMapping],
    ) -> list[ValidationIssue]:
        """检查单选/多选列的唯一值分布（INFO 级别）"""
        issues = []
        select_types = {LarkFieldType.SINGLE_SELECT, LarkFieldType.MULTI_SELECT}

        for m in field_mappings:
            if m.lark_type not in select_types:
                continue

            col = df[m.source_col]
            non_null = col.dropna()
            if non_null.empty:
                continue

            unique_vals = non_null.unique()
            # 限制记录的唯一值数量，避免日志过长
            display_vals = unique_vals[:20]
            issues.append(
                ValidationIssue(
                    level="INFO",
                    field=m.source_col,
                    message=(
                        f"Select column has {len(unique_vals)} unique values. "
                        f"Sample: {list(display_vals)}"
                    ),
                )
            )

        return issues
