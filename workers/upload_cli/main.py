# coding:utf8
"""workers.upload_cli.main -- 通用本地文件上传飞书多维表格 CLI"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date as _date
import logging
from pathlib import Path
import sys
from typing import Any

import pandas as pd

from automation.client import LarkMultiDimTable
from automation.conf import lark as lark_conf
from automation.utils.common.attachment import normalize_attachment_input

from workers.lib import AttachmentTokenResolver, FieldTypeCoercer
from workers.lib.file_extractor import extract_single_local_source
from workers.lib.local_attachment_preprocessor import preprocess_local_attachment_columns
from workers.lib.models import CleanupCondition, FieldMapping, LarkFieldType, LarkTargetConfig, LocalFileSourceConfig
from workers.lib.lark_loader import _write_single_target

logger = logging.getLogger("workers.upload_cli.main")

DEFAULT_MAPPING_THRESHOLD = 0.9
DEFAULT_ATTACHMENT_BAK_SUFFIX = "_bak_raw"
WRITE_BATCH_SIZE = 500
DEFAULT_ATTACHMENT_MAX_SIZE_MB = 19


@dataclass
class MappingOverride:
    source_col: str
    target_field: str
    lark_type: LarkFieldType | None = None


def _init_lark_client() -> LarkMultiDimTable:
    app_id = lark_conf.get("prod", "APP_ID")
    app_secret = lark_conf.get("prod", "APP_SECRET")
    lark_host = lark_conf.get("prod", "LARK_HOST", fallback="https://open.feishu.cn")
    logger.info("Initializing Lark client (app_id=%s, host=%s)", app_id, lark_host)
    return LarkMultiDimTable(app_id=app_id, app_secret=app_secret, lark_host=lark_host)


def _parse_csv_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _parse_sheet_name(raw: str) -> Any:
    text = raw.strip()
    if text.isdigit():
        return int(text)
    return text


def _parse_type_name(raw: str) -> LarkFieldType:
    key = raw.strip().lower()
    mapping = {
        "text": LarkFieldType.TEXT,
        "string": LarkFieldType.TEXT,
        "number": LarkFieldType.NUMBER,
        "float": LarkFieldType.NUMBER,
        "int": LarkFieldType.NUMBER,
        "date": LarkFieldType.DATE,
        "datetime": LarkFieldType.DATE,
        "url": LarkFieldType.URL,
        "attachment": LarkFieldType.ATTACHMENT,
        "file": LarkFieldType.ATTACHMENT,
        "checkbox": LarkFieldType.CHECKBOX,
        "bool": LarkFieldType.CHECKBOX,
        "single_select": LarkFieldType.SINGLE_SELECT,
        "multi_select": LarkFieldType.MULTI_SELECT,
    }
    if key not in mapping:
        raise ValueError(f"Unsupported lark type in override: {raw}")
    return mapping[key]


def _parse_mapping_overrides(raw_items: list[str]) -> dict[str, MappingOverride]:
    overrides: dict[str, MappingOverride] = {}
    for item in raw_items:
        parts = [p.strip() for p in item.split(":")]
        if len(parts) < 2 or len(parts) > 3:
            raise ValueError(
                f"Invalid --field-map value '{item}'. Use source_col:target_field[:type]"
            )
        source_col, target_field = parts[0], parts[1]
        lark_type = _parse_type_name(parts[2]) if len(parts) == 3 else None
        if not source_col or not target_field:
            raise ValueError(f"Invalid --field-map value '{item}'")
        overrides[source_col] = MappingOverride(
            source_col=source_col,
            target_field=target_field,
            lark_type=lark_type,
        )
    return overrides


def _infer_lark_type(
    *,
    col_name: str,
    series: pd.Series,
    attachment_cols: set[str],
    cleanup_date_field: str | None,
) -> LarkFieldType:
    if col_name in attachment_cols:
        return LarkFieldType.ATTACHMENT
    if cleanup_date_field and col_name == cleanup_date_field:
        return LarkFieldType.DATE

    if pd.api.types.is_datetime64_any_dtype(series):
        return LarkFieldType.DATE
    if pd.api.types.is_bool_dtype(series):
        return LarkFieldType.CHECKBOX
    if pd.api.types.is_numeric_dtype(series):
        return LarkFieldType.NUMBER
    return LarkFieldType.TEXT


def _resolve_table_fields(
    client: LarkMultiDimTable,
    *,
    table_url: str,
    table_name: str,
) -> tuple[bool, str | None, dict[str, dict]]:
    client.extract_app_information(url=table_url)
    app_token = client.app_token
    tables_map = client.extract_table_information(app_token=app_token)
    table_id = tables_map.get(table_name)
    if table_id is None:
        return (False, None, {})

    field_items = client.list_fields(table_id=table_id)
    field_map = {
        item.get("field_name"): item
        for item in field_items
        if isinstance(item, dict) and item.get("field_name")
    }
    return (True, table_id, field_map)


def _has_link_like_value(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip().lower()
    if not text:
        return False
    prefixes = ("http://", "https://", "//", "/", "./", "../", "~")
    return text.startswith(prefixes)


def _warn_unconfigured_attachment_columns(
    df: pd.DataFrame,
    *,
    attachment_cols: set[str],
) -> None:
    for col in df.columns:
        if col in attachment_cols:
            continue
        sample = df[col].dropna().head(50)
        if sample.empty:
            continue
        if sample.map(_has_link_like_value).any():
            logger.warning(
                "Column '%s' contains URL/path-like values but is not in --attachment-cols; "
                "it will be uploaded as normal field (no attachment upload).",
                col,
            )


def _apply_attachment_bak_columns(
    df: pd.DataFrame,
    *,
    attachment_cols: set[str],
    bak_suffix: str,
    enabled: bool,
) -> pd.DataFrame:
    if not enabled or not attachment_cols:
        return df

    out = df.copy()
    for col in attachment_cols:
        if col not in out.columns:
            continue
        bak_col = f"{col}{bak_suffix}"
        out[bak_col] = ""
    return out


def _is_size_limit_error(error_text: str | None) -> bool:
    if not error_text:
        return False
    lowered = error_text.lower()
    return "size limit" in lowered or "exceeds size limit" in lowered


def _prepare_attachment_columns_for_upload(
    df: pd.DataFrame,
    *,
    attachment_cols: set[str],
    attachment_resolver: AttachmentTokenResolver,
    enable_attachment_bak: bool,
    bak_suffix: str,
    attachment_separator: str,
) -> tuple[pd.DataFrame, int]:
    if not attachment_cols:
        return df, 0

    out = df.copy()
    oversized_failed_count = 0
    for col in attachment_cols:
        if col not in out.columns:
            continue

        # Attachment values are normalized to list[str], so ensure object dtype.
        out[col] = out[col].astype(object)

        bak_col = f"{col}{bak_suffix}"
        has_bak_col = enable_attachment_bak and bak_col in out.columns

        for idx, raw_value in out[col].items():
            urls = normalize_attachment_input(raw_value)
            if not urls:
                out.at[idx, col] = []
                if has_bak_col:
                    out.at[idx, bak_col] = ""
                continue

            success_urls: list[str] = []
            oversized_failed_urls: list[str] = []
            for url in urls:
                token = attachment_resolver.resolve_single(url)
                if token:
                    success_urls.append(url)
                    continue

                reason = attachment_resolver.get_failed_reason(url)
                if _is_size_limit_error(reason):
                    oversized_failed_urls.append(url)

            out.at[idx, col] = success_urls
            if has_bak_col:
                out.at[idx, bak_col] = (
                    attachment_separator.join(oversized_failed_urls)
                    if oversized_failed_urls
                    else ""
                )
            oversized_failed_count += len(oversized_failed_urls)

    return out, oversized_failed_count


def _build_cleanup_condition(
    df: pd.DataFrame,
    *,
    date_field: str | None,
    cleanup_mode: str,
    date_value: str | None,
    start_offset: int,
    end_offset: int,
) -> CleanupCondition | None:
    if not date_field:
        return None
    if date_field not in df.columns:
        raise ValueError(f"cleanup date field '{date_field}' not found in source data")

    mode = cleanup_mode.lower()
    if mode == "offset":
        if not date_value:
            raise ValueError("cleanup_mode=offset requires --date")
        if start_offset > end_offset:
            raise ValueError("cleanup window invalid: start_offset must <= end_offset")
        ref = _date.fromisoformat(date_value)
        start_date = ref + pd.Timedelta(days=start_offset)
        end_date = ref + pd.Timedelta(days=end_offset)
        return CleanupCondition.date_window(date_field, start_date, end_date)

    if mode == "data":
        date_values = pd.to_datetime(df[date_field], errors="coerce").dropna()
        if date_values.empty:
            raise ValueError(
                f"cleanup_mode=data but source column '{date_field}' has no valid dates"
            )
        start_date = date_values.min().date()
        end_date = date_values.max().date()
        logger.info(
            "Resolved cleanup window from source data: %s -> %s (field=%s)",
            start_date,
            end_date,
            date_field,
        )
        return CleanupCondition.date_window(date_field, start_date, end_date)

    raise ValueError(f"Unsupported cleanup_mode: {cleanup_mode}")


def _build_field_mappings(
    df: pd.DataFrame,
    *,
    attachment_cols: set[str],
    cleanup_date_field: str | None,
    overrides: dict[str, MappingOverride],
    target_fields: dict[str, dict],
    table_exists: bool,
    bak_suffix: str,
    allow_create_missing_fields: bool,
) -> tuple[list[FieldMapping], float, list[str], list[str], int, int]:
    mappings: list[FieldMapping] = []
    unmatched: list[str] = []
    missing_target_fields: list[str] = []

    source_cols = list(df.columns)
    local_columns = [col for col in source_cols if col != "row_key" and not col.endswith(bak_suffix)]
    local_field_count = len(local_columns)
    same_name_matched_count = sum(1 for col in local_columns if col in target_fields)

    for col in source_cols:
        if col == "row_key":
            continue
        override = overrides.get(col)
        target_field = override.target_field if override else col

        target_missing = table_exists and target_field not in target_fields
        if target_missing and not allow_create_missing_fields:
            unmatched.append(col)
            continue

        if override and override.lark_type is not None:
            lark_type = override.lark_type
        elif table_exists and not target_missing:
            lark_type = LarkFieldType(int(target_fields[target_field]["type"]))
        else:
            lark_type = _infer_lark_type(
                col_name=col,
                series=df[col],
                attachment_cols=attachment_cols,
                cleanup_date_field=cleanup_date_field,
            )
            if target_missing:
                missing_target_fields.append(target_field)

        mappings.append(
            FieldMapping(
                source_col=col,
                target_field=target_field,
                lark_type=lark_type,
            )
        )

    coverage = 1.0 if local_field_count == 0 else same_name_matched_count / local_field_count
    return (
        mappings,
        coverage,
        unmatched,
        missing_target_fields,
        same_name_matched_count,
        local_field_count,
    )


def _collect_attachment_stats(
    df: pd.DataFrame,
    *,
    attachment_cols: set[str],
) -> tuple[int, int]:
    non_empty_rows = 0
    items = 0
    for col in attachment_cols:
        if col not in df.columns:
            continue
        for value in df[col].tolist():
            if isinstance(value, list):
                if value:
                    non_empty_rows += 1
                    items += len(value)
                continue
            if value is None:
                continue
            text = str(value).strip()
            if text:
                non_empty_rows += 1
                items += 1
    return (non_empty_rows, items)


def run_upload_pipeline(args: argparse.Namespace) -> int:
    logger.info("=" * 60)
    logger.info("Upload CLI Pipeline - START")
    logger.info("=" * 60)

    attachment_cols = set(_parse_csv_list(args.attachment_cols))
    allowed_roots = _parse_csv_list(args.allowed_roots)
    if not allowed_roots:
        allowed_roots = [str(Path(args.file).expanduser().resolve().parent)]

    overrides = _parse_mapping_overrides(args.field_map or [])

    try:
        logger.info("[Step 1/4] Loading local data file...")
        local_cfg = LocalFileSourceConfig(
            name="upload_cli_source",
            path=args.file,
            format=args.file_format,
            sheet_name=_parse_sheet_name(args.sheet_name),
            encoding=args.encoding,
            delimiter=args.delimiter,
            allowed_roots=allowed_roots,
            attachment_columns=sorted(attachment_cols),
            multi_value_separator=args.attachment_separator,
            json_array_enabled=True,
        )
        source_df = extract_single_local_source(local_cfg)
        source_df = preprocess_local_attachment_columns(source_df, local_cfg)
        _warn_unconfigured_attachment_columns(source_df, attachment_cols=attachment_cols)
        source_df = _apply_attachment_bak_columns(
            source_df,
            attachment_cols=attachment_cols,
            bak_suffix=args.attachment_bak_suffix,
            enabled=args.enable_attachment_bak,
        )

        if args.row_key_col:
            if args.row_key_col not in source_df.columns:
                raise ValueError(f"row_key column not found: {args.row_key_col}")
            source_df = source_df.assign(row_key=source_df[args.row_key_col].astype(str))
            if source_df["row_key"].eq("").any():
                raise ValueError("row_key contains empty values")

        non_empty_rows, items = _collect_attachment_stats(source_df, attachment_cols=attachment_cols)
        logger.info(
            "Source ready: rows=%s cols=%s attachment_rows=%s attachment_items=%s",
            source_df.shape[0],
            source_df.shape[1],
            non_empty_rows,
            items,
        )
    except Exception as exc:
        logger.error("[Step 1/4] Failed to prepare local data: %s", exc)
        return 1

    try:
        logger.info("[Step 2/4] Inspecting target table fields...")
        lark_client = _init_lark_client()
        table_exists, table_id, target_fields = _resolve_table_fields(
            lark_client,
            table_url=args.table_url,
            table_name=args.table_name,
        )
        logger.info(
            "Target table status: exists=%s table_id=%s target_fields=%s",
            table_exists,
            table_id,
            len(target_fields),
        )
    except Exception as exc:
        logger.error("[Step 2/4] Failed to inspect target table: %s", exc)
        return 1

    try:
        logger.info("[Step 3/4] Building field mappings and validating coverage...")
        (
            mappings,
            coverage,
            unmatched,
            missing_target_fields,
            same_name_matched_count,
            local_field_count,
        ) = _build_field_mappings(
            source_df,
            attachment_cols=attachment_cols,
            cleanup_date_field=args.cleanup_date_field,
            overrides=overrides,
            target_fields=target_fields,
            table_exists=table_exists,
            bak_suffix=args.attachment_bak_suffix,
            allow_create_missing_fields=args.allow_create_missing_fields,
        )

        logger.info(
            "Mapping result: mapped=%s local_fields=%s same_name_matched=%s coverage=%.2f%% unmatched=%s",
            len(mappings),
            local_field_count,
            same_name_matched_count,
            coverage * 100,
            len(unmatched),
        )

        if table_exists and coverage < args.mapping_threshold:
            reason = (
                "Mapping coverage too low for existing table: "
                f"coverage={coverage:.2%}({same_name_matched_count}/{local_field_count}), "
                f"threshold={args.mapping_threshold:.2%}. "
                "Formula=同名字段数/本地字段数. "
                f"unmatched_columns={unmatched}"
            )
            logger.error(reason)
            return 1

        if not mappings:
            raise ValueError("No field mappings resolved; nothing to upload")

        if args.validate_mapping_only:
            logger.info("validate-mapping-only enabled, stopping after mapping validation")
            return 0
    except Exception as exc:
        logger.error("[Step 3/4] Mapping validation failed: %s", exc)
        return 1

    try:
        logger.info("[Step 4/4] Uploading data...")
        if not args.enable_upload:
            logger.error(
                "upload step is disabled in current branch runtime. "
                "Use --validate-mapping-only for threshold validation."
            )
            return 1

        cleanup_condition = _build_cleanup_condition(
            source_df,
            date_field=args.cleanup_date_field,
            cleanup_mode=args.cleanup_mode,
            date_value=args.date,
            start_offset=args.start_offset,
            end_offset=args.end_offset,
        )

        target = LarkTargetConfig(
            name="upload_cli_target",
            url=args.table_url,
            table_name=args.table_name,
            field_mappings=mappings,
            cleanup_conditions=cleanup_condition,
        )

        app_token = lark_client.app_token
        if not app_token:
            raise ValueError("missing app_token after table inspection")

        attachment_resolver = AttachmentTokenResolver(
            client=lark_client,
            app_token=app_token,
            max_size_mb=args.attachment_max_size_mb,
            max_retries=2,
            backoff_seconds=0.4,
            target_name=target.name,
        )
        prepared_df, oversized_failed_count = _prepare_attachment_columns_for_upload(
            source_df,
            attachment_cols=attachment_cols,
            attachment_resolver=attachment_resolver,
            enable_attachment_bak=args.enable_attachment_bak,
            bak_suffix=args.attachment_bak_suffix,
            attachment_separator=args.attachment_separator,
        )
        coercer = FieldTypeCoercer(
            attachment_resolver=attachment_resolver.resolve_single,
        )
        written_count = _write_single_target(
            lark_client,
            target,
            prepared_df,
            coercer=coercer,
            validator=None,
            validation_level="skip",
        )

        logger.info(
            "Upload completed: rows=%s table=%s attachment_cache=%s oversized_failed=%s",
            written_count,
            args.table_name,
            len(attachment_resolver._token_cache),
            oversized_failed_count,
        )
        return 0
    except Exception as exc:
        logger.error("[Step 4/4] Upload failed: %s", exc)
        return 1


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Upload local CSV/Excel to Feishu bitable")

    parser.add_argument("--file", required=True, help="本地文件路径（csv/xlsx/xls/tsv/txt）")
    parser.add_argument("--file-format", default="auto", help="文件格式：auto/csv/tsv/txt/xlsx/xls")
    parser.add_argument("--sheet-name", default="0", help="Excel sheet 名称或索引")
    parser.add_argument("--encoding", default="utf-8", help="文本文件编码")
    parser.add_argument("--delimiter", default=None, help="文本分隔符，默认按格式推断")
    parser.add_argument(
        "--allowed-roots",
        default="",
        help="允许读取本地路径白名单，逗号分隔；为空时默认文件所在目录",
    )

    parser.add_argument("--table-url", required=True, help="飞书多维表格 URL")
    parser.add_argument("--table-name", required=True, help="目标表名称")

    parser.add_argument(
        "--field-map",
        action="append",
        default=[],
        help="字段映射覆盖，可重复传入：source_col:target_field[:type]",
    )
    parser.add_argument(
        "--mapping-threshold",
        type=float,
        default=DEFAULT_MAPPING_THRESHOLD,
        help="已存在目标表时，自动映射覆盖率阈值（0~1）",
    )
    parser.add_argument(
        "--allow-create-missing-fields",
        action="store_true",
        help="目标表已存在时，允许把缺失字段自动创建到目标表",
    )
    parser.add_argument(
        "--validate-mapping-only",
        action="store_true",
        help="仅做映射率和匹配校验，不执行清理和上传",
    )
    parser.add_argument(
        "--enable-upload",
        action="store_true",
        help="显式开启上传步骤（默认关闭，当前分支建议仅用于校验）",
    )
    parser.add_argument(
        "--validation-level",
        default="warn",
        choices=["strict", "warn", "skip"],
        help="写入前 schema 校验级别",
    )

    parser.add_argument(
        "--cleanup-date-field",
        default=None,
        help="清理窗口日期字段（参数优先，不做自动推断）",
    )
    parser.add_argument(
        "--cleanup-mode",
        default="data",
        choices=["data", "offset"],
        help="data=按源数据日期窗口清理；offset=按 date+offset 清理",
    )
    parser.add_argument("--date", default=None, help="基准日期 YYYY-MM-DD（cleanup_mode=offset 时必填）")
    parser.add_argument("--start-offset", type=int, default=-7, help="清理窗口起始偏移")
    parser.add_argument("--end-offset", type=int, default=0, help="清理窗口结束偏移")

    parser.add_argument(
        "--attachment-cols",
        default="",
        help="附件列名，逗号分隔；仅显式声明列会执行附件上传",
    )
    parser.add_argument(
        "--attachment-separator",
        default=",",
        help="附件多值分隔符（例如 , 或 |）",
    )
    parser.add_argument(
        "--attachment-max-size-mb",
        type=int,
        default=DEFAULT_ATTACHMENT_MAX_SIZE_MB,
        help="附件下载大小限制（MB）",
    )
    parser.add_argument(
        "--enable-attachment-bak",
        action="store_true",
        help="为附件源列新增 bak 字段，保留原始值",
    )
    parser.add_argument(
        "--attachment-bak-suffix",
        default=DEFAULT_ATTACHMENT_BAK_SUFFIX,
        help="附件 bak 列后缀",
    )

    parser.add_argument("--row-key-col", default=None, help="row_key 来源列")

    return parser


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = build_arg_parser()
    args = parser.parse_args()

    if not 0 <= args.mapping_threshold <= 1:
        parser.error("--mapping-threshold must be between 0 and 1")

    code = run_upload_pipeline(args)
    sys.exit(code)


if __name__ == "__main__":
    main()
