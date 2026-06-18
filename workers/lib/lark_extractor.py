# coding:utf8
"""workers.lib.lark_extractor -- 通用飞书多维表格数据拉取

封装对 LarkMultiDimTable 客户端的调用，按 LarkSourceConfig 列表批量拉取数据。
本模块不含任何业务字段名或业务表名，通用性通过配置对象保证。

调用链路：
  extract_app_information(url)
    -> extract_table_information(app_token)  得到 {table_name: table_id}
    -> request_records_generator(table_id, view_id, page_size)
    -> 日期筛选（可选）
    -> 转换为 DataFrame
"""

import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from .models import LarkSourceConfig, LarkFieldType

logger = logging.getLogger("workers.lib.lark_extractor")


def extract_all_lark_sources(
    client,
    sources: list[LarkSourceConfig]
) -> dict[str, pd.DataFrame]:
    """批量拉取多个飞书多维表格数据源

    Args:
        client:  LarkMultiDimTable 客户端实例
        sources: LarkSourceConfig 配置列表

    Returns:
        dict[str, pd.DataFrame]：{source.name -> DataFrame}
        每个 DataFrame 包含该源表的所有记录（字段名为飞书原始字段名）
    """
    result = {}
    for source in sources:
        logger.info(f"Extracting Lark source: {source.name} from {source.url}")
        try:
            df = extract_single_source(client, source)
            result[source.name] = df
            logger.info(f"Lark source '{source.name}' extracted: {len(df)} rows")
        except Exception as e:
            logger.error(f"Failed to extract Lark source '{source.name}': {e}")
            raise
    return result


def extract_single_source(
    client,
    source: LarkSourceConfig
) -> pd.DataFrame:
    """拉取单个飞书多维表格数据源

    步骤：
    1. 解析 URL 获取 app_token
    2. 获取表格列表，按 table_name 匹配 table_id
    3. 分页拉取所有记录
    4. 可选：按日期字段筛选最近 N 天
    5. 转换为 DataFrame

    Args:
        client: LarkMultiDimTable 客户端实例
        source: LarkSourceConfig 配置

    Returns:
        pd.DataFrame：包含所有记录的 DataFrame，列名为飞书字段名

    Raises:
        ValueError: 当 table_name 在目标文档中不存在时
        Exception:  Lark API 调用失败时
    """
    # 1. 解析 URL，获取 app_token 和表格列表
    client.extract_app_information(url=source.url)
    app_token = client.app_token
    tables_map = client.extract_table_information(app_token=app_token)

    # 2. 按 table_name 匹配 table_id
    table_id = tables_map.get(source.table_name)
    if table_id is None:
        available = list(tables_map.keys())
        raise ValueError(
            f"Table '{source.table_name}' not found in app_token={app_token}. "
            f"Available tables: {available}"
        )

    logger.info(
        f"Source '{source.name}': app_token={app_token}, "
        f"table_id={table_id}, view_id={source.view_id}"
    )

    # 3. 获取字段元数据 + 分页拉取所有记录
    field_type_map, all_records = _fetch_all_records(
        client,
        table_id=table_id,
        view_id=source.view_id,
        field_names=source.field_names,
    )

    # 4. 转换为 DataFrame
    df = _records_to_dataframe(all_records, field_type_map)

    # 5. 可选日期筛选
    if source.date_filter_field and (
        source.date_filter_start_date is not None
        or source.date_filter_end_date is not None
    ):
        df = _apply_date_filter(
            df,
            source.date_filter_field,
            start_date=source.date_filter_start_date,
            end_date=source.date_filter_end_date,
        )

    return df


def _fetch_all_records(
    client,
    table_id: str,
    view_id: Optional[str] = None,
    page_size: int = 100,
    field_names: Optional[list[str]] = None,
) -> tuple[dict[str, LarkFieldType], list[dict]]:
    """分页拉取指定表格的所有记录，同时返回字段类型映射

    Args:
        client:      LarkMultiDimTable 客户端
        table_id:    表格 ID
        view_id:     视图 ID（可选）
        page_size:   每页记录数，最大 500
        field_names: 限定拉取的字段名列表，None 表示拉取所有字段

    Returns:
        tuple[field_type_map, all_records]
        - field_type_map: {field_name: LarkFieldType} 字段类型映射
        - all_records:    所有记录，格式 [{"record_id": "...", "fields": {...}}, ...]
    """
    # 获取字段元数据，构建 {field_name: LarkFieldType} 映射
    raw_fields = client.list_fields(table_id=table_id)
    field_type_map: dict[str, LarkFieldType] = {}
    for f in raw_fields:
        name = f.get("field_name")
        ftype = f.get("type")
        if name and ftype is not None:
            try:
                field_type_map[name] = LarkFieldType(ftype)
            except ValueError:
                logger.debug(f"Unknown field type {ftype} for field '{name}', skipping")
    logger.debug(f"Field type map: {len(field_type_map)} fields loaded")

    all_records: list[dict] = []

    # 构建 request_records_generator 的额外参数
    gen_kwargs = {}
    if field_names:
        gen_kwargs["field_names"] = field_names

    records_gen = client.request_records_generator(
        table_id=table_id,
        view_id=view_id,
        page_size=min(page_size, 500),
        **gen_kwargs,
    )

    for resp in records_gen:
        code = resp.get("code", -1)
        if code != 0:
            msg = resp.get("msg", "Unknown error")
            logger.error(f"Failed to fetch records: code={code}, msg={msg}")
            raise RuntimeError(f"Lark API error while fetching records: [{code}] {msg}")

        items = resp.get("data", {}).get("items", [])
        all_records.extend(items)
        logger.debug(f"Fetched {len(items)} records, total: {len(all_records)}")

    return field_type_map, all_records


def _records_to_dataframe(
    records: list[dict],
    field_type_map: Optional[dict[str, LarkFieldType]] = None,
) -> pd.DataFrame:
    """将飞书记录列表转换为 DataFrame

    基于 field_type_map 中的 LarkFieldType 枚举对字段值进行精确标准化。
    当 field_type_map 为 None 时回退到 duck-typing 模式。
    """
    if not records:
        return pd.DataFrame()

    if field_type_map is None:
        field_type_map = {}

    rows = []
    for record in records:
        row = {"_record_id": record.get("record_id")}
        fields = record.get("fields", {})
        for field_name, field_value in fields.items():
            ftype = field_type_map.get(field_name)
            row[field_name] = _normalize_field_value(field_value, ftype)
        rows.append(row)

    df = pd.DataFrame(rows)
    logger.info(f"Converted {len(records)} records to DataFrame with {len(df.columns)} columns")
    return df


def _normalize_field_value(value, field_type: Optional[LarkFieldType] = None):
    """对飞书字段值进行标准化处理

    当提供 field_type 时按枚举精确分发，否则回退到 duck-typing。

    飞书字段类型与值格式（参考 LarkFieldType 枚举）：
      - LarkFieldType.TEXT(1)          → str
      - LarkFieldType.NUMBER(2)        → float/int
      - LarkFieldType.SINGLE_SELECT(3) → {"text": "...", "link": "..."}
      - LarkFieldType.MULTI_SELECT(4)  → [{"text": "..."}, ...]
      - LarkFieldType.DATE(5)          → int（毫秒时间戳）
      - LarkFieldType.CHECKBOX(7)      → bool
      - LarkFieldType.PERSON(11)       → [{"id": "...", "name": "..."}]
      - LarkFieldType.CREATED_TIME(1001) / MODIFIED_TIME(1002) → int（毫秒时间戳）
      - LarkFieldType.CREATED_USER(1003) / MODIFIED_USER(1004) → [{"id": "...", "name": "..."}]
    """
    if value is None:
        return None

    # ── 基于 field_type 精确分发 ──────────────────────────────────
    if field_type is not None:
        # 日期类字段（DATE / CREATED_TIME / MODIFIED_TIME）→ datetime
        if field_type in (LarkFieldType.DATE, LarkFieldType.CREATED_TIME, LarkFieldType.MODIFIED_TIME):
            if isinstance(value, (int, float)):
                try:
                    return pd.Timestamp(value, unit="ms")
                except Exception:
                    return value

        # 单选字段 → 提取 text
        elif field_type == LarkFieldType.SINGLE_SELECT:
            if isinstance(value, dict):
                return value.get("text")

        # 多选字段 → 提取 text 列表
        elif field_type == LarkFieldType.MULTI_SELECT:
            if isinstance(value, list):
                return [item.get("text") for item in value if isinstance(item, dict)]

        # 人员/创建人/修改人 → 提取 id 列表
        elif field_type in (LarkFieldType.PERSON, LarkFieldType.CREATED_USER, LarkFieldType.MODIFIED_USER):
            if isinstance(value, list):
                return [item.get("id") for item in value if isinstance(item, dict)]

        # NUMBER 类型：飞书 API 可能以字符串形式返回数值
        elif field_type == LarkFieldType.NUMBER:
            if isinstance(value, str):
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return value
            return value

        # 文本/复选框/公式等 → 直接返回原值
        else:
            return value

    # ── 回退：duck-typing（field_type 未知时） ──────────────────────
    # 毫秒时间戳（日期 / 创建时间 / 更新时间）
    if isinstance(value, (int, float)) and value > 9999999999:
        try:
            return pd.Timestamp(value, unit="ms")
        except Exception:
            return value

    # 单选字段 {"text": "...", "link": "..."}
    if isinstance(value, dict) and "text" in value and len(value) <= 3:
        return value.get("text")

    # 多选字段 [{"text": "..."}, ...]
    if isinstance(value, list) and value and isinstance(value[0], dict) and "text" in value[0]:
        return [item.get("text") for item in value]

    # 人员字段 [{"id": "...", "name": "..."}]
    if isinstance(value, list) and value and isinstance(value[0], dict) and "id" in value[0]:
        return [item.get("id") for item in value]

    return value


def _apply_date_filter(
    df: pd.DataFrame,
    date_field: str,
    *,
    start_date=None,
    end_date=None,
) -> pd.DataFrame:
    """按日期字段筛选数据，支持显式日期区间

    支持三种日期格式：
    - datetime64 / Timestamp：直接使用
    - Excel 序列号（int/float，如 46167 = 2026-05-28）：用 origin='1899-12-30', unit='D' 转换
    - 字符串日期：用 pd.to_datetime 解析

    过滤模式：
    - start_date 不为 None：下界 = start_date
    - end_date 不为 None：上界 = end_date（含当天 23:59:59）
    - 两者都为 None：不做任何过滤

    设计说明：
        显式 start_date/end_date 是唯一支持的用法，由调用方（如
        _apply_date_range_to_lark_sources）基于 reference_date 计算好
        精确日期后传入，确保 Lark 源过滤窗口与 SQL DATEADD 窗口完全一致。

    Args:
        df:         原始 DataFrame
        date_field: 日期字段名
        start_date: 可选，date 或 datetime 对象，过滤下界（含）。
        end_date:   可选，date 或 datetime 对象，过滤上界（含）。

    Returns:
        筛选后的 DataFrame
    """
    if date_field not in df.columns:
        logger.warning(
            f"Date filter field '{date_field}' not found in DataFrame. "
            f"Available columns: {list(df.columns)}. Skipping date filter."
        )
        return df

    original_len = len(df)

    # ── 计算下界 cutoff ──────────────────────────────────────────────
    if start_date is not None:
        # 显式下界：由调用方基于 reference_date 精确计算
        if isinstance(start_date, datetime):
            cutoff = start_date
        else:
            cutoff = datetime.combine(start_date, datetime.min.time())
    elif end_date is not None:
        # 仅有上界，无下界 → 不做下界过滤
        cutoff = None
    else:
        # 无下界也无上界 → 不做过滤
        logger.info(f"Date filter on '{date_field}': no start_date/end_date, skipping")
        return df

    # 检测 Excel 序列号：整数/浮点列且值在合理范围内（1~100000 ≈ 1900~2174 年）
    raw_col = df[date_field]
    if pd.api.types.is_integer_dtype(raw_col) or pd.api.types.is_float_dtype(raw_col):
        sample_max = raw_col.dropna().max() if not raw_col.dropna().empty else 0
        if 0 < sample_max < 100000:
            logger.info(
                f"Date filter: '{date_field}' detected as Excel serial number "
                f"(max={sample_max}), converting with origin='1899-12-30'"
            )
            date_col = pd.to_datetime(
                raw_col, origin="1899-12-30", unit="D", errors="coerce"
            )
        else:
            # 可能是毫秒时间戳等其他数值格式
            date_col = pd.to_datetime(raw_col, errors="coerce")
    else:
        date_col = pd.to_datetime(raw_col, errors="coerce")

    # 将转换后的日期写回 DataFrame，确保下游拿到的已经是标准 datetime64
    df = df.copy()
    df[date_field] = date_col

    mask = pd.Series(True, index=df.index)

    # 下界过滤
    if cutoff is not None:
        mask = date_col >= cutoff

    # 上界过滤：当 end_date 有值时，额外添加上界约束
    if end_date is not None:
        if isinstance(end_date, datetime):
            end_dt = end_date
        else:
            end_dt = datetime.combine(end_date, datetime.max.time().replace(microsecond=0))
        mask = mask & (date_col <= end_dt)
        end_str = end_dt.strftime('%Y-%m-%d')
    else:
        end_str = "(no upper limit)"

    filtered = df[mask].copy()

    cutoff_str = cutoff.strftime('%Y-%m-%d') if cutoff is not None else "(no lower limit)"
    logger.info(
        f"Date filter on '{date_field}': kept {len(filtered)}/{original_len} rows "
        f"(cutoff={cutoff_str}, end={end_str}, "
        f"mode={'explicit' if start_date is not None else 'end_only'})"
    )
    return filtered
