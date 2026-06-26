# coding:utf8
"""workers.lib.file_extractor -- 本地文件源抽取

提供与 lark_extractor / mc_extractor 同构的抽取接口：
  - extract_single_local_source
  - extract_all_local_sources

支持格式：csv/tsv/txt/xlsx/xls（auto 自动推断）。
"""

from __future__ import annotations

import logging
from pathlib import Path
import re

import pandas as pd

from .models import LocalFileSourceConfig

logger = logging.getLogger("workers.lib.file_extractor")


def _resolve_path(raw_path: str) -> Path:
    p = Path(raw_path).expanduser()
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    else:
        p = p.resolve()
    return p


def _validate_allowed_path(path: Path, config: LocalFileSourceConfig) -> None:
    mode = (config.path_match_mode or "prefix").lower()
    rules = [r for r in config.allowed_roots if isinstance(r, str) and r.strip()]
    if not rules:
        raise ValueError(
            f"Local source '{config.name}' requires non-empty allowed_roots for safety"
        )

    path_str = str(path)
    matched = False

    if mode == "prefix":
        for rule in rules:
            root = _resolve_path(rule)
            root_str = str(root)
            if path_str == root_str or path_str.startswith(root_str + "/"):
                matched = True
                break
    elif mode == "glob":
        for rule in rules:
            if path.match(rule):
                matched = True
                break
    elif mode == "regex":
        for rule in rules:
            if re.search(rule, path_str):
                matched = True
                break
    else:
        raise ValueError(
            f"Local source '{config.name}': unsupported path_match_mode={config.path_match_mode}"
        )

    if not matched:
        raise PermissionError(
            f"Local source '{config.name}': path not allowed by rules ({mode}): {path_str}"
        )


def _detect_format(path: Path, configured: str) -> str:
    fmt = (configured or "auto").lower()
    if fmt != "auto":
        return fmt
    ext = path.suffix.lower().lstrip(".")
    if ext in {"csv", "tsv", "txt", "xlsx", "xls"}:
        return ext
    raise ValueError(f"Cannot infer local file format from suffix: {path}")


def extract_single_local_source(config: LocalFileSourceConfig) -> pd.DataFrame:
    """抽取单个本地文件源并返回 DataFrame。"""
    path = _resolve_path(config.path)
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(
            f"Local source '{config.name}': file not found: {path}"
        )

    _validate_allowed_path(path, config)
    fmt = _detect_format(path, config.format)

    logger.info(
        "Extracting local source '%s': path=%s, format=%s",
        config.name,
        path,
        fmt,
    )

    if fmt in {"csv", "tsv", "txt"}:
        sep = config.delimiter
        if sep is None:
            sep = "\t" if fmt in {"tsv", "txt"} else ","
        text_header = "infer" if config.header == 0 else config.header
        kwargs = {
            "sep": sep,
            "encoding": config.encoding,
            "quotechar": config.quotechar,
            "header": text_header,
            "skiprows": config.skiprows,
            "usecols": config.usecols,
            "parse_dates": config.parse_dates,
            "dtype": config.dtype,
        }
        kwargs.update(config.read_kwargs or {})
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        df = pd.read_csv(path, **kwargs)
    elif fmt in {"xlsx", "xls"}:
        kwargs = {
            "sheet_name": config.sheet_name,
            "header": config.header,
            "skiprows": config.skiprows,
            "usecols": config.usecols,
            "parse_dates": config.parse_dates,
            "dtype": config.dtype,
        }
        kwargs.update(config.read_kwargs or {})
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        df = pd.read_excel(path, **kwargs)
    else:
        raise ValueError(
            f"Local source '{config.name}': unsupported file format '{fmt}'"
        )

    if not isinstance(df, pd.DataFrame):
        raise TypeError(
            f"Local source '{config.name}': expected DataFrame, got {type(df).__name__}"
        )

    logger.info(
        "Local source '%s' extracted: %s rows, %s columns",
        config.name,
        df.shape[0],
        df.shape[1],
    )
    return df


def extract_all_local_sources(
    configs: list[LocalFileSourceConfig],
) -> dict[str, pd.DataFrame]:
    """批量抽取本地文件源。"""
    result: dict[str, pd.DataFrame] = {}
    for cfg in configs:
        result[cfg.name] = extract_single_local_source(cfg)
    return result
