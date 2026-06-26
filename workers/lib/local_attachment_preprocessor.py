# coding:utf8
"""workers.lib.local_attachment_preprocessor -- 本地文件源附件中间处理器

在应用层对本地文件源中的附件列做预处理：
1. 统一解析输入（本地路径 / URL / 逗号分隔 / JSON 数组）
2. 可选补全不完整 URL（通过 url_prefix_map）
3. 本地路径白名单校验
4. 输出为 list[str] 以供后续 coercer/resolver 使用
"""

from __future__ import annotations

import ast
import json
import logging
from pathlib import Path
import re
from typing import Any

import pandas as pd

from .models import LocalFileSourceConfig

logger = logging.getLogger("workers.lib.local_attachment_preprocessor")


def _resolve_path(raw_path: str) -> Path:
    p = Path(raw_path).expanduser()
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    else:
        p = p.resolve()
    return p


def _match_allowed(path: Path, rules: list[str], mode: str) -> bool:
    path_str = str(path)
    if mode == "prefix":
        for rule in rules:
            root = _resolve_path(rule)
            root_str = str(root)
            if path_str == root_str or path_str.startswith(root_str + "/"):
                return True
        return False
    if mode == "glob":
        return any(path.match(rule) for rule in rules)
    if mode == "regex":
        return any(re.search(rule, path_str) is not None for rule in rules)
    raise ValueError(f"Unsupported path_match_mode={mode}")


def _is_http_url(value: str) -> bool:
    lower = value.lower()
    return lower.startswith("http://") or lower.startswith("https://")


def _apply_url_prefix(value: str, prefix_map: dict[str, str]) -> str:
    text = value.strip()
    for key, prefix in prefix_map.items():
        if not key:
            continue
        if text.startswith(key):
            suffix = text[len(key):]
            return prefix + suffix
    if text.startswith("//"):
        return "https:" + text
    return text


def _parse_multi_values(value: Any, separator: str, json_array_enabled: bool) -> list[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []

    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []

        if json_array_enabled and text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if str(v).strip()]
            except Exception:
                try:
                    parsed = ast.literal_eval(text)
                    if isinstance(parsed, list):
                        return [str(v).strip() for v in parsed if str(v).strip()]
                except Exception:
                    pass

        if separator and separator in text:
            return [part.strip() for part in text.split(separator) if part.strip()]

        return [text]

    return [str(value).strip()]


def preprocess_local_attachment_columns(
    df: pd.DataFrame,
    config: LocalFileSourceConfig,
) -> pd.DataFrame:
    """对配置中的附件列执行中间预处理，返回新 DataFrame。"""
    if df.empty or not config.attachment_columns:
        return df

    rules = [r for r in config.allowed_roots if isinstance(r, str) and r.strip()]
    if not rules:
        raise ValueError(
            f"Local source '{config.name}' requires allowed_roots for attachment safety"
        )

    mode = (config.path_match_mode or "prefix").lower()
    prefix_map = config.url_prefix_map or {}
    result = df.copy()

    for col in config.attachment_columns:
        if col not in result.columns:
            logger.warning(
                "Local source '%s': attachment column '%s' not found, skip",
                config.name,
                col,
            )
            continue

        def _normalize_cell(cell: Any) -> list[str]:
            items = _parse_multi_values(
                cell,
                separator=config.multi_value_separator,
                json_array_enabled=config.json_array_enabled,
            )
            normalized: list[str] = []
            for item in items:
                candidate = _apply_url_prefix(item, prefix_map)
                if _is_http_url(candidate):
                    normalized.append(candidate)
                    continue

                local_path = _resolve_path(candidate)
                if not _match_allowed(local_path, rules, mode):
                    raise PermissionError(
                        f"Local source '{config.name}': attachment path not allowed: {local_path}"
                    )
                if not local_path.exists() or not local_path.is_file():
                    raise FileNotFoundError(
                        f"Local source '{config.name}': attachment file not found: {local_path}"
                    )
                normalized.append(str(local_path))
            return normalized

        result[col] = result[col].apply(_normalize_cell)

    return result
