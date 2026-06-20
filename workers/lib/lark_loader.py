# coding:utf8
"""workers.lib.lark_loader -- 通用飞书写入/清理

封装对 LarkMultiDimTable 客户端的写入和清理操作，按 LarkTargetConfig 列表批量写入。
本模块不含任何业务字段名或业务表名，通用性通过配置对象保证。

写入流程：
  对每个 LarkTargetConfig：
    1. 解析目标 URL，获取 app_token 和 table_id
    2. 按 cleanup_conditions 清理旧数据（可选）
    3. 将 DataFrame 按 field_mappings 转换为 records
    4. 分批写入（add_batch_records）
"""

import logging
import time
from typing import Optional

import pandas as pd

from .models import (
    LarkTargetConfig,
    FieldMapping,
    CleanupCondition,
    LarkFieldType,
    FilterOperator,
    _to_lark_timestamp,
)

logger = logging.getLogger("workers.lib.lark_loader")

# 飞书 API 限制：单次批量写入最多 500 条，删除最多 500 条
WRITE_BATCH_SIZE = 500
DELETE_BATCH_SIZE = 500

# 飞书索引字段支持的类型（第一个字段自动成为索引字段）
# 参考: https://open.feishu.cn/document/server-docs/docs/bitable-v1/app-table/create
_INDEX_FIELD_TYPES = {
    LarkFieldType.TEXT,  # 1
    LarkFieldType.NUMBER,  # 2
    LarkFieldType.DATE,  # 5
    LarkFieldType.PHONE,  # 13
    LarkFieldType.URL,  # 15
    LarkFieldType.FORMULA,  # 20
    LarkFieldType.LOCATION,  # 22
}


def _build_fields_from_mappings(mappings: list[FieldMapping]) -> list[dict]:
    """将 FieldMapping 列表转换为飞书 create_table 所需的 fields 参数

    每个 FieldMapping 转为 {"field_name": target_field, "type": lark_type}。
    第一个字段自动成为索引字段，若其类型不在 _INDEX_FIELD_TYPES 中，
    则将其降级为 TEXT 类型（type=1）以满足索引字段约束。

    Args:
        mappings: FieldMapping 列表（来自 LarkTargetConfig.field_mappings）

    Returns:
        list[dict]：飞书 create_table API 所需的 fields 参数
    """
    if not mappings:
        raise ValueError("field_mappings cannot be empty when auto-creating a table")

    fields = []
    for i, m in enumerate(mappings):
        field_def = {
            "field_name": m.target_field,
            "type": int(m.lark_type),
        }
        # 第一个字段是索引字段，类型受限
        if i == 0 and field_def["type"] not in _INDEX_FIELD_TYPES:
            logger.warning(
                f"Index field '{m.target_field}' has type {m.lark_type}, "
                f"which is not supported for index fields. Downgrading to TEXT (type=1)."
            )
            field_def["type"] = int(LarkFieldType.TEXT)
        fields.append(field_def)

    return fields


def write_to_all_targets(
    client,
    result_df: pd.DataFrame,
    targets: list[LarkTargetConfig],
    coercer=None,
    validator=None,
    validation_level: str = "warn",
) -> None:
    """将结果 DataFrame 写入多个飞书目标表

    Args:
        client:            LarkMultiDimTable 客户端实例
        result_df:         融合/转换后的结果 DataFrame
        targets:           LarkTargetConfig 配置列表
        coercer:           FieldTypeCoercer 实例，若为 None 则假定 result_df 已是 records 格式
        validator:         SchemaValidator 实例（可选），用于写入前校验
        validation_level:  校验级别 "strict" | "warn" | "skip"

    Raises:
        Exception: 写入失败时
    """
    for target in targets:
        logger.info(f"Writing to target: {target.name} ({target.url})")
        try:
            _write_single_target(
                client,
                target,
                result_df,
                coercer,
                validator=validator,
                validation_level=validation_level,
            )
            logger.info(f"Target '{target.name}' write completed successfully")
        except Exception as e:
            logger.error(f"Failed to write to target '{target.name}': {e}")
            raise


def _write_single_target(
    client,
    target: LarkTargetConfig,
    result_df: pd.DataFrame,
    coercer=None,
    validator=None,
    validation_level: str = "warn",
) -> None:
    """写入单个目标表

    步骤：
    1. 解析目标 URL
    2. 清理旧数据
    3. 写入前校验（可选）
    4. 转换并写入
    """
    # 1. 解析 URL，获取 app_token 和 table_id
    try:
        client.extract_app_information(url=target.url)
        app_token = client.app_token
    except Exception as e:
        raise ValueError(
            f"Failed to parse target URL '{target.url}': {e}. "
            f"Check that the app has read permission on this wiki/base node."
        ) from e

    try:
        tables_map = client.extract_table_information(app_token=app_token)
    except Exception as e:
        raise ValueError(
            f"Failed to extract table info from app_token={app_token}: {e}"
        ) from e

    table_id = tables_map.get(target.table_name)
    if table_id is None:
        # 表不存在，自动创建
        fields = _build_fields_from_mappings(target.field_mappings)
        logger.info(
            f"Target table '{target.table_name}' not found in app_token={app_token}, "
            f"auto-creating with {len(fields)} fields: "
            f"{[f['field_name'] for f in fields[:5]]}..."
        )
        try:
            table_id = client.create_table(
                name=target.table_name,
                fields=fields,
                app_token=app_token,
            )
            logger.info(f"Target '{target.name}': table created, table_id={table_id}")
        except Exception as e:
            raise ValueError(
                f"Failed to auto-create target table '{target.table_name}' "
                f"in app_token={app_token}: {e}"
            ) from e

    logger.info(f"Target '{target.name}': app_token={app_token}, table_id={table_id}")

    # 2. 清理旧数据
    if target.cleanup_conditions is not None:
        deleted_count = cleanup_target_table(client, target, table_id)
        logger.info(f"Target '{target.name}': cleaned up {deleted_count} old records")

    # 3. 写入前校验（可选）
    if validator is not None and validation_level != "skip":
        report = validator.validate(
            result_df, target.field_mappings, target_name=target.name
        )
        logger.info(report.summary)
        if report.has_critical and validation_level == "strict":
            details = report.format_details(min_level="CRITICAL")
            raise ValueError(
                f"Pre-write validation failed for target '{target.name}' (strict mode):\n{details}"
            )
        if report.has_critical:
            logger.warning(
                f"Target '{target.name}': validation has CRITICAL issues "
                f"but validation_level='{validation_level}', continuing..."
            )

    # 4. 转换 DataFrame 为 records 格式
    if coercer is not None:
        records = coercer.apply_to_dataframe(result_df, target.field_mappings)
    else:
        # 假定 result_df 已经是 records 格式（list of {"fields": {...}}）
        records = (
            result_df if isinstance(result_df, list) else result_df.to_dict("records")
        )

    # 4. 分批写入
    _write_records_batched(client, table_id, target.name, records)


def _extract_date_range(cleanup_cond: CleanupCondition):
    """从 CleanupCondition 提取日期范围，用于客户端过滤

    当 cleanup 条件为双边日期范围（IS_GREATER + IS_LESS，conjunction=and）时，
    提取 (field_name, lower_ms, upper_ms) 元组，供客户端过滤使用。
    其他条件类型返回 None，回退到服务端 filter。

    Returns:
        (field_name, lower_ms, upper_ms) 或 None
    """
    if not cleanup_cond.conditions or cleanup_cond.conjunction != "and":
        return None
    lower_ms = upper_ms = field_name = None
    for cond in cleanup_cond.conditions:
        if cond.operator == FilterOperator.IS_GREATER and cond.value:
            field_name = cond.field_name
            lower_ms = _to_lark_timestamp(cond.value[0])
        elif cond.operator == FilterOperator.IS_LESS and cond.value:
            field_name = cond.field_name
            upper_ms = _to_lark_timestamp(cond.value[0])
    if field_name and lower_ms is not None and upper_ms is not None:
        return (field_name, lower_ms, upper_ms)
    return None


def _do_cleanup(
    client,
    target: LarkTargetConfig,
    table_id: str,
) -> int:
    """执行单次清理操作（fetch + filter + delete）

    当 cleanup 条件为日期范围时，使用 GET 接口拉取全表记录并在客户端过滤，
    避免飞书 POST search API 对大表查询挂死的问题。
    对于非日期范围条件，回退到原有的 POST search + filter 逻辑。

    Args:
        client:    LarkMultiDimTable 客户端
        target:    LarkTargetConfig 配置
        table_id:  目标表 ID

    Returns:
        int：删除的记录数
    """
    cleanup_cond = target.cleanup_conditions
    filter_obj = cleanup_cond.to_lark_filter()
    date_range = _extract_date_range(cleanup_cond)

    if date_range:
        field_name, lower_ms, upper_ms = date_range
        logger.info(
            f"Cleaning up target '{target.name}': using client-side filtering, "
            f"{field_name} in [{lower_ms}, {upper_ms})"
        )
        # GET all records (no server-side filter), filter locally
        records_gen = client.request_records_generator(
            table_id=table_id,
            page_size=500,
        )
    else:
        logger.info(f"Cleaning up target '{target.name}': filter={filter_obj}")
        # Non-date-range condition: use POST search with filter
        records_gen = client.request_records_generator(
            table_id=table_id,
            filter=filter_obj,
            page_size=500,
        )

    # 分页收集待删除的 record IDs
    all_record_ids = []
    for resp in records_gen:
        if resp.get("code", -1) != 0:
            msg = resp.get("msg", "Unknown error")
            logger.error(
                f"Failed to fetch records for cleanup: [{resp.get('code')}] {msg}"
            )
            raise RuntimeError(
                f"Lark API error during cleanup fetch: [{resp.get('code')}] {msg}"
            )

        items = resp.get("data", {}).get("items", [])
        if not items:
            # 空页早停
            break

        for item in items:
            rid = item.get("record_id")
            if not rid:
                continue
            if date_range:
                # 客户端过滤：仅收集日期范围 [lower_ms, upper_ms) 内的记录
                record_date = item.get("fields", {}).get(field_name)
                if record_date is not None and lower_ms <= record_date < upper_ms:
                    all_record_ids.append(rid)
            else:
                all_record_ids.append(rid)

    if not all_record_ids:
        logger.info(f"No records to clean up in target '{target.name}'")
        return 0

    # 批量删除
    deleted_count = 0
    for i in range(0, len(all_record_ids), DELETE_BATCH_SIZE):
        batch = all_record_ids[i : i + DELETE_BATCH_SIZE]
        try:
            client.delete_batch_records(batch, table_id=table_id)
            deleted_count += len(batch)
            logger.debug(
                f"Deleted batch {i // DELETE_BATCH_SIZE + 1}: {len(batch)} records"
            )
        except Exception as e:
            logger.error(f"Failed to delete batch {i // DELETE_BATCH_SIZE + 1}: {e}")
            raise
        time.sleep(1)  # 避免限流

    return deleted_count


def cleanup_target_table(
    client,
    target: LarkTargetConfig,
    table_id: Optional[str] = None,
) -> int:
    """按 cleanup_conditions 清理目标表中的旧记录

    使用 GET + 客户端过滤代替 POST search API，避免飞书 search 接口
    对大表日期过滤查询挂死的问题（实测 cat4 表 1200+ 行挂死 98 分钟）。

    当遇到 token 过期错误（code 99991663）时，自动刷新 token 并重试一次。

    Args:
        client:    LarkMultiDimTable 客户端
        target:    LarkTargetConfig 配置（cleanup_conditions 不为 None）
        table_id:  目标表 ID（若为 None 则从 URL 解析）

    Returns:
        int：删除的记录数
    """
    cleanup_cond = target.cleanup_conditions
    if cleanup_cond is None:
        return 0

    # 如果 table_id 未提供，从 URL 解析
    if table_id is None:
        client.extract_app_information(url=target.url)
        tables_map = client.extract_table_information(app_token=client.app_token)
        table_id = tables_map.get(target.table_name)
        if table_id is None:
            raise ValueError(f"Table '{target.table_name}' not found")

    try:
        return _do_cleanup(client, target, table_id)
    except RuntimeError as e:
        if "99991663" not in str(e):
            raise
        logger.warning(
            f"Token expired during cleanup of '{target.name}', "
            f"refreshing and retrying..."
        )
        client._refresh_access_token()
        return _do_cleanup(client, target, table_id)


def _write_records_batched(
    client,
    table_id: str,
    target_name: str,
    records: list[dict],
) -> None:
    """分批写入记录到飞书多维表格

    Args:
        client:      LarkMultiDimTable 客户端
        table_id:    目标表 ID
        target_name: 目标逻辑名（用于日志）
        records:     records 列表，每个元素格式 {"fields": {...}}

    Raises:
        RuntimeError: 写入失败时
    """
    if not records:
        logger.info(f"Target '{target_name}': no records to write")
        return

    total = len(records)
    success_count = 0
    failed_batches = []

    for i in range(0, total, WRITE_BATCH_SIZE):
        batch = records[i : i + WRITE_BATCH_SIZE]
        batch_num = i // WRITE_BATCH_SIZE + 1
        total_batches = (total + WRITE_BATCH_SIZE - 1) // WRITE_BATCH_SIZE

        logger.info(
            f"Target '{target_name}': writing batch {batch_num}/{total_batches} "
            f"({len(batch)} records, total progress: {i + len(batch)}/{total})"
        )

        try:
            client.add_batch_records(batch, table_id=table_id)
            success_count += len(batch)
        except Exception as e:
            logger.error(
                f"Target '{target_name}': batch {batch_num} failed: {e}. "
                f"Records {i+1}-{i+len(batch)} not written."
            )
            failed_batches.append((batch_num, len(batch), str(e)))

        # 批次间间隔，避免限流
        if i + WRITE_BATCH_SIZE < total:
            time.sleep(2)

    # 汇总结果
    if failed_batches:
        error_summary = "; ".join(
            f"batch {bn} ({cnt} records): {err}" for bn, cnt, err in failed_batches
        )
        raise RuntimeError(
            f"Target '{target_name}': {len(failed_batches)} batch(es) failed. "
            f"Written: {success_count}/{total}. Errors: {error_summary}"
        )

    logger.info(
        f"Target '{target_name}': all {success_count} records written successfully"
    )
