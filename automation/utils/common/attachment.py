# coding:utf8
"""Common helpers for attachment URL normalization and local downloads."""

from __future__ import annotations

import ast
import hashlib
import json
import logging
import mimetypes
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

DEFAULT_TIMEOUT = (10, 60)
ALLOWED_SCHEMES = {"http", "https"}
logger = logging.getLogger("automation.utils.common.attachment")


@dataclass(frozen=True)
class ParsedAttachmentURL:
    raw_value: Any
    normalized_urls: list[str]


def normalize_attachment_input(value: Any) -> list[str]:
    """Normalize attachment input into a list of http(s) URLs.

    Accepted shapes:
    - single URL string
    - list/tuple/set of URL strings
    - stringified JSON/python list, e.g. '["https://..."]' or "['https://...']"
    """
    if value is None:
        return []

    if isinstance(value, (list, tuple, set)):
        candidates = list(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            candidates = _parse_stringified_list(text)
            if not candidates:
                logger.warning("Malformed bracket attachment input ignored: %s", text)
        else:
            candidates = [text]
    else:
        candidates = [str(value)]

    normalized: list[str] = []
    for item in candidates:
        if item is None:
            continue
        if isinstance(item, str):
            candidate = item.strip().strip("\"').").strip()
        else:
            candidate = str(item).strip()
        if not candidate:
            continue
        if _is_valid_http_url(candidate):
            normalized.append(candidate)
    return normalized


def _parse_stringified_list(text: str) -> list[Any]:
    try:
        parsed = json.loads(text)
    except Exception:
        try:
            parsed = ast.literal_eval(text)
        except Exception:
            logger.debug("Failed to parse stringified list input: %s", text)
            parsed = []
    if isinstance(parsed, (list, tuple, set)):
        return list(parsed)
    if isinstance(parsed, str):
        return [parsed]
    return []


def _is_valid_http_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in ALLOWED_SCHEMES and bool(parsed.netloc)


def guess_media_type(url: str, content_type: str | None = None) -> str:
    if content_type:
        lowered = content_type.lower().split(";")[0].strip()
        if lowered.startswith("image/"):
            return "image"
        if lowered.startswith("video/"):
            return "video"
    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix in {
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".webp",
        ".bmp",
        ".ico",
        ".tiff",
        ".heic",
    }:
        return "image"
    if suffix in {".mp4", ".mov", ".m4v", ".webm", ".avi", ".mkv"}:
        return "video"
    guessed = mimetypes.guess_type(url)[0] or ""
    if guessed.startswith("image/"):
        return "image"
    if guessed.startswith("video/"):
        return "video"
    return "unknown"


def normalize_filename(url: str, content_type: str | None = None) -> str:
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
    media_type = guess_media_type(url, content_type)
    suffix = ".bin"
    if media_type == "image":
        suffix = Path(urlparse(url).path).suffix or ".jpg"
    elif media_type == "video":
        suffix = Path(urlparse(url).path).suffix or ".mp4"
    return f"attachment_{digest}{suffix}"


def download_url_to_tempfile(
    url: str, *, timeout=DEFAULT_TIMEOUT, max_size_mb: int | None = None
) -> tuple[str, str | None]:
    """Download a remote URL to a temp file and return (path, content_type)."""
    if not _is_valid_http_url(url):
        raise ValueError(f"Unsupported attachment URL scheme: {url}")

    response = requests.get(url, stream=True, timeout=timeout, allow_redirects=True)
    response.raise_for_status()

    content_type = response.headers.get("Content-Type")
    suffix = Path(urlparse(url).path).suffix
    fd, temp_path = tempfile.mkstemp(prefix="lark_attachment_", suffix=suffix or ".bin")
    total_size = 0
    try:
        with os.fdopen(fd, "wb") as writer:
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                total_size += len(chunk)
                if max_size_mb is not None and total_size > max_size_mb * 1024 * 1024:
                    raise ValueError(f"Attachment exceeds size limit: {max_size_mb} MB")
                writer.write(chunk)
    except Exception:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise

    return temp_path, content_type


def safe_remove_file(file_path: str) -> None:
    try:
        os.remove(file_path)
    except OSError:
        pass
