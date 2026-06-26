# coding:utf8
"""workers.lib.router -- 数据路由执行器

根据 DataRoute 配置列表，将数据从统一数据池分发到多个飞书目标表。
支持 per-route 的数据转换、写入前校验和差异化处理。

核心设计：统一数据池
- 数据池（data_pool）合并 lark_data、mc_data、result_df 为统一引用字典
- 数据池可选合并 file_data（本地文件源）
- 多个路由引用同一 source_ref 时，共享同一份 DataFrame 对象（零拷贝）
- per-route transforms 接收 DataFrame 副本（.copy()），避免路由间互相污染

数据流转：
    data_pool = {"result": df, "lark:xxx": df, "mc:yyy": df, "file:zzz": df}
    → Route A: source="mc:yyy" → transforms → validate → write → target_a
    → Route B: source="mc:yyy" → transforms → validate → write → target_b  (共享源数据)
    → Route C: source="result" → transforms → validate → write → target_c
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd

from .models import DataRoute, LarkTargetConfig
from .validator import SchemaValidator, ValidationReport
from .lark_loader import _write_single_target

logger = logging.getLogger("workers.lib.router")


# --------------------------------------------------------------------------
# 路由执行报告
# --------------------------------------------------------------------------


@dataclass
class RouteResult:
    """单条路由的执行结果"""

    route_name: str
    source_ref: str
    source_shape: tuple
    final_shape: tuple
    validation_report: Optional[ValidationReport] = None
    success: bool = True
    error: Optional[str] = None


@dataclass
class RouteReport:
    """所有路由的执行结果汇总"""

    results: list[RouteResult] = field(default_factory=list)

    @property
    def total_routes(self) -> int:
        return len(self.results)

    @property
    def success_count(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def failed_count(self) -> int:
        return sum(1 for r in self.results if not r.success)

    @property
    def summary(self) -> str:
        return (
            f"Route report: {self.success_count}/{self.total_routes} succeeded, "
            f"{self.failed_count} failed"
        )


# --------------------------------------------------------------------------
# 路由执行器
# --------------------------------------------------------------------------


class DataRouter:
    """数据路由执行器

    职责：
    1. 构建统一数据池（data_pool），合并 lark_data、mc_data、result_df
    2. 按 DataRoute 配置从池中选取数据
    3. 应用 per-route 转换（聚合、过滤等）
    4. 执行写入前校验
    5. 委托 lark_loader 写入目标

    使用方式：
        router = DataRouter(lark_client, coercer)
        report = router.route(
            DATA_ROUTES,
            lark_data=lark_data,
            mc_data=mc_data,
            result_df=result_df,
        )
        logger.info(report.summary)
    """

    def __init__(
        self,
        lark_client,
        coercer,
        validator: Optional[SchemaValidator] = None,
        attachment_resolver=None,
        persistence_config=None,
    ):
        """
        Args:
            lark_client: LarkMultiDimTable 客户端实例
            coercer:     FieldTypeCoercer 实例
            validator:   SchemaValidator 实例，None 时自动创建
        """
        self._lark_client = lark_client
        self._coercer = coercer
        self._validator = validator or SchemaValidator()
        self._attachment_resolver = attachment_resolver
        self._persistence_config = persistence_config

    def route(
        self,
        routes: list[DataRoute],
        *,
        lark_data: dict[str, pd.DataFrame],
        mc_data: dict[str, pd.DataFrame],
        file_data: Optional[dict[str, pd.DataFrame]] = None,
        result_df: Optional[pd.DataFrame] = None,
    ) -> RouteReport:
        """执行所有路由

        Args:
            routes:    DataRoute 配置列表
            lark_data: {source.name -> DataFrame} 飞书数据源字典
            mc_data:   {query.name -> DataFrame} MaxCompute 查询结果字典
            file_data: {source.name -> DataFrame} 本地文件源字典（可选）
            result_df: 主管道 merge + transform 后的结果 DataFrame（可选）

        Returns:
            RouteReport：所有路由的执行结果汇总
        """
        if not routes:
            logger.info("No routes to execute")
            return RouteReport()

        # 1. 构建统一数据池（零拷贝，只存引用）
        data_pool = self._build_data_pool(lark_data, mc_data, file_data, result_df)
        logger.info(
            f"Data pool built with {len(data_pool)} entries: "
            f"{list(data_pool.keys())}"
        )

        # 2. 逐路由执行（隔离失败：单路由失败不阻断后续独立路由）
        report = RouteReport()
        for route in routes:
            result = self._execute_single_route(route, data_pool)
            report.results.append(result)
            if not result.success:
                logger.error(f"Route '{route.name}' failed: {result.error}")
                # 不 raise，继续执行后续路由

        logger.info(report.summary)

        # 全部路由执行完毕后，如有失败路由则汇总报告
        if report.failed_count > 0:
            failed_names = [r.route_name for r in report.results if not r.success]
            raise RuntimeError(
                f"{report.failed_count}/{report.total_routes} route(s) failed: "
                f"{', '.join(failed_names)}"
            )

        return report

    def _build_data_pool(
        self,
        lark_data: dict[str, pd.DataFrame],
        mc_data: dict[str, pd.DataFrame],
        file_data: Optional[dict[str, pd.DataFrame]],
        result_df: Optional[pd.DataFrame],
    ) -> dict[str, pd.DataFrame]:
        """构建统一数据池

        将所有数据源合并为一个 dict，key 格式：
        - "result"      → result_df
        - "lark:<name>" → lark_data[name]
        - "mc:<name>"   → mc_data[name]
        - "file:<name>" → file_data[name]
        """
        pool: dict[str, pd.DataFrame] = {}
        if result_df is not None:
            pool["result"] = result_df
        for name, df in lark_data.items():
            pool[f"lark:{name}"] = df
        for name, df in mc_data.items():
            pool[f"mc:{name}"] = df
        if file_data:
            for name, df in file_data.items():
                pool[f"file:{name}"] = df
        return pool

    def _execute_single_route(
        self,
        route: DataRoute,
        data_pool: dict[str, pd.DataFrame],
    ) -> RouteResult:
        """执行单条路由：选数据 → 转换 → 校验 → 写入"""
        logger.info(f"--- Route '{route.name}' START ---")

        try:
            # 1. 解析数据源
            source_df = self._resolve_source(route.source_ref, data_pool)
            source_shape = source_df.shape
            logger.info(
                f"Route '{route.name}': source='{route.source_ref}' "
                f"({source_shape[0]} rows, {source_shape[1]} cols)"
            )

            # 2. 应用 per-route 转换（传入副本，保护原始数据）
            if route.transforms:
                df = source_df.copy()
                for i, transform in enumerate(route.transforms):
                    before_shape = df.shape
                    df = transform(df)
                    logger.info(
                        f"Route '{route.name}': transform[{i}] "
                        f"{before_shape} -> {df.shape}"
                    )
                logger.info(f"Route '{route.name}': after all transforms {df.shape}")
            else:
                df = source_df

            # 3. 写入前校验
            validation_report = None
            if route.validation_level != "skip":
                validation_report = self._validator.validate(
                    df, route.target.field_mappings, target_name=route.name
                )
                logger.info(validation_report.summary)

                if (
                    validation_report.has_critical
                    and route.validation_level == "strict"
                ):
                    details = validation_report.format_details(min_level="CRITICAL")
                    error_msg = f"Validation failed (strict mode):\n{details}"
                    return RouteResult(
                        route_name=route.name,
                        source_ref=route.source_ref,
                        source_shape=source_shape,
                        final_shape=df.shape,
                        validation_report=validation_report,
                        success=False,
                        error=error_msg,
                    )

                # warn 模式下记录警告但不阻断
                if validation_report.has_critical:
                    logger.warning(
                        f"Route '{route.name}': validation has CRITICAL issues "
                        f"but validation_level='{route.validation_level}', continuing..."
                    )

            # 3.5 写入前补齐缺失列（当 validation_level != 'strict' 时）
            #     若 merge 跳过了某些源导致 result_df 缺少 field_mappings 要求的列，
            #     自动填充 NaN 以避免 coercer 报错，确保独立路由仍能写入。
            if route.validation_level != "strict":
                missing_cols = [
                    m.source_col
                    for m in route.target.field_mappings
                    if m.source_col not in df.columns
                ]
                if missing_cols:
                    logger.warning(
                        f"Route '{route.name}': padding {len(missing_cols)} missing "
                        f"columns with NaN: {missing_cols[:5]}{'...' if len(missing_cols) > 5 else ''}"
                    )
                    if df is source_df:
                        df = df.copy()  # 避免修改共享数据
                    for col in missing_cols:
                        df[col] = float("nan")

            # 4. 写入目标表（复用 lark_loader 的 _write_single_target）
            _write_single_target(
                self._lark_client,
                route.target,
                df,
                coercer=self._coercer,
                attachment_resolver=self._attachment_resolver,
                persistence_config=self._persistence_config,
            )

            logger.info(f"--- Route '{route.name}' COMPLETED ---")
            return RouteResult(
                route_name=route.name,
                source_ref=route.source_ref,
                source_shape=source_shape,
                final_shape=df.shape,
                validation_report=validation_report,
                success=True,
            )

        except Exception as e:
            logger.error(f"Route '{route.name}' failed: {e}")
            return RouteResult(
                route_name=route.name,
                source_ref=route.source_ref,
                source_shape=source_df.shape if "source_df" in dir() else (0, 0),
                final_shape=(0, 0),
                success=False,
                error=str(e),
            )

    def _resolve_source(
        self,
        source_ref: str,
        data_pool: dict[str, pd.DataFrame],
    ) -> pd.DataFrame:
        """解析 source_ref 字符串，从数据池中返回对应 DataFrame

        Args:
            source_ref: 数据源引用字符串
            data_pool:  统一数据池

        Returns:
            pd.DataFrame

        Raises:
            KeyError: source_ref 在数据池中不存在
        """
        if source_ref not in data_pool:
            available = list(data_pool.keys())
            raise KeyError(
                f"Data source '{source_ref}' not found in data pool. "
                f"Available: {available}"
            )
        return data_pool[source_ref]

    def describe_routes(
        self,
        routes: list[DataRoute],
        data_pool: Optional[dict[str, pd.DataFrame]] = None,
    ) -> str:
        """生成路由配置的文本摘要（可视化能力）

        Args:
            routes:    DataRoute 配置列表
            data_pool: 可选的数据池，用于显示源数据的实际行数

        Returns:
            str: 格式化的路由摘要
        """
        lines = [f"=== Data Routes ({len(routes)} routes) ==="]

        for route in routes:
            lines.append(f"\n[Route: {route.name}]")
            lines.append(f"  Source: {route.source_ref}")

            # 如果有数据池，显示实际行数
            if data_pool and route.source_ref in data_pool:
                df = data_pool[route.source_ref]
                lines.append(f"  Source rows: {len(df)}")

            lines.append(f"  Target: {route.target.name} → {route.target.table_name}")
            lines.append(f"  Transforms: {len(route.transforms)} step(s)")
            lines.append(f"  Validation: {route.validation_level}")

            # 字段类型统计
            type_counts: dict[str, int] = {}
            for m in route.target.field_mappings:
                type_name = (
                    m.lark_type.name
                    if hasattr(m.lark_type, "name")
                    else str(m.lark_type)
                )
                type_counts[type_name] = type_counts.get(type_name, 0) + 1
            type_summary = ", ".join(
                f"{k} x{v}" for k, v in sorted(type_counts.items())
            )
            lines.append(
                f"  Fields: {len(route.target.field_mappings)} mappings ({type_summary})"
            )

            if route.target.cleanup_conditions:
                lines.append("  Cleanup: enabled")

        return "\n".join(lines)
