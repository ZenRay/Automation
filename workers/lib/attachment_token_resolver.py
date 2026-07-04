# coding:utf8
"""Attachment resolution service for workers.

Resolve attachment URLs into Feishu Bitable attachment payloads by:
1. normalizing user input
2. downloading remote material to a temp file
3. uploading the temp file through automation client
4. extracting file_token and caching per batch
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any

from automation.utils.common.attachment import (
    DEFAULT_TIMEOUT,
    download_url_to_tempfile,
    guess_media_type,
    normalize_attachment_input,
    normalize_filename,
    safe_remove_file,
)
from .route_write_persistence import RouteWritePersistence

logger = logging.getLogger("workers.lib.attachment_token_resolver")


RETRYABLE_EXCEPTIONS = (TimeoutError, ConnectionError)


def _extract_file_token(payload: Any) -> str | None:
    if payload is None:
        return None
    if isinstance(payload, str):
        return payload
    if isinstance(payload, dict):
        for key in ("file_token", "fileToken", "token"):
            value = payload.get(key)
            if isinstance(value, str) and value:
                return value
        for value in payload.values():
            found = _extract_file_token(value)
            if found:
                return found
    if isinstance(payload, list):
        for item in payload:
            found = _extract_file_token(item)
            if found:
                return found
    return None


@dataclass
class AttachmentTokenResolver:
    client: Any
    app_token: str | None = None
    timeout: tuple[int, int] = DEFAULT_TIMEOUT
    max_size_mb: int | None = None
    max_retries: int = 2
    backoff_seconds: float = 0.4
    target_name: str = ""
    field_name: str = ""
    row_key_getter: Any = None
    persistence: RouteWritePersistence | None = None
    _token_cache: dict[str, str] = field(default_factory=dict, init=False)
    _failed_reason_cache: dict[str, str] = field(default_factory=dict, init=False)
    _cache_lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _stats_total: int = field(default=0, init=False)
    _stats_done: int = field(default=0, init=False)
    _stats_failed: int = field(default=0, init=False)
    _stats_start_time: float = field(default=0.0, init=False)

    def resolve(self, value: Any) -> list[dict]:
        urls = normalize_attachment_input(value)
        row_key = self._get_row_key(value)
        parse_error = None
        if (
            isinstance(value, str)
            and value.strip().startswith("[")
            and value.strip().endswith("]")
            and not urls
        ):
            parse_error = "Malformed bracket attachment input"

        if self.persistence is not None:
            self.persistence.append_input_snapshot(
                target_name=self.target_name,
                row_key=row_key,
                field_name=self.field_name,
                raw_value=value,
            )
            self.persistence.append_parsed_snapshot(
                target_name=self.target_name,
                row_key=row_key,
                field_name=self.field_name,
                normalized_urls=urls,
                parse_status="success" if urls else "failed",
                parse_error=parse_error,
            )

        if not urls:
            if parse_error:
                logger.warning(
                    "Attachment parse failed target=%s field=%s row_key=%s raw=%s",
                    self.target_name,
                    self.field_name,
                    row_key,
                    value,
                )
            return []

        resolved: list[dict] = []

        for url in urls:
            file_token = self.resolve_single(url, row_key=row_key)
            if file_token:
                resolved.append({"file_token": file_token})
        return resolved

    def resolve_single(self, url: str, *, row_key: str = "") -> str | None:
        with self._cache_lock:
            if url in self._token_cache:
                return self._token_cache[url]

        last_error = None
        for attempt in range(self.max_retries + 1):
            temp_path = None
            try:
                temp_path, content_type = download_url_to_tempfile(
                    url,
                    timeout=self.timeout,
                    max_size_mb=self.max_size_mb,
                )
                file_name = normalize_filename(url, content_type)
                mime_type = content_type
                media_type = guess_media_type(url, content_type)
                if media_type == "unknown":
                    logger.warning("Unknown attachment media type for url=%s", url)

                parent_type = (
                    "bitable_image" if media_type == "image" else "bitable_file"
                )

                try:
                    response = self.client.upload_attachment(
                        temp_path,
                        app_token=self.app_token,
                        file_name=file_name,
                        mime_type=mime_type,
                        need_binary=True,
                        parent_type=parent_type,
                    )
                except TypeError as type_error:
                    # Backward compatible with test doubles or legacy clients
                    # that do not expose the parent_type keyword argument.
                    if "parent_type" not in str(type_error):
                        raise
                    response = self.client.upload_attachment(
                        temp_path,
                        app_token=self.app_token,
                        file_name=file_name,
                        mime_type=mime_type,
                        need_binary=True,
                    )
                file_token = _extract_file_token(response)
                if not file_token:
                    raise ValueError(
                        f"Attachment upload response missing file_token: {response}"
                    )

                with self._cache_lock:
                    self._token_cache[url] = file_token
                    self._failed_reason_cache.pop(url, None)
                if self.persistence is not None:
                    self.persistence.append_upload_event(
                        target_name=self.target_name,
                        row_key=row_key,
                        field_name=self.field_name,
                        normalized_url=url,
                        upload_status="success",
                        retry_count=attempt,
                        file_token=file_token,
                        error_type=None,
                        error_message=None,
                        retryable=None,
                    )
                return file_token
            except Exception as exc:
                last_error = exc
                retryable = self._is_retryable(exc)
                if self.persistence is not None:
                    self.persistence.append_upload_event(
                        target_name=self.target_name,
                        row_key=row_key,
                        field_name=self.field_name,
                        normalized_url=url,
                        upload_status="failed",
                        retry_count=attempt,
                        file_token=None,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                        retryable=retryable,
                    )
                if not retryable or attempt >= self.max_retries:
                    break
                time.sleep(self.backoff_seconds * (2**attempt))
            finally:
                if temp_path:
                    safe_remove_file(temp_path)

        logger.error("Resolve attachment failed for url=%s: %s", url, last_error)
        with self._cache_lock:
            self._failed_reason_cache[url] = (
                str(last_error) if last_error is not None else ""
            )
        return None

    def seed_token_cache(self, token_map: dict[str, str]) -> None:
        if not token_map:
            return
        with self._cache_lock:
            self._token_cache.update(token_map)

    def get_failed_reason(self, url: str) -> str | None:
        with self._cache_lock:
            return self._failed_reason_cache.get(url)

    def _get_row_key(self, value: Any) -> str:
        if callable(self.row_key_getter):
            try:
                result = self.row_key_getter(value)
                if result is not None:
                    return str(result)
            except Exception:
                pass
        return ""

    def resolve_batch(
        self, url_list: list[str], *, concurrency: int = 4
    ) -> dict[str, str]:
        """并行预解析所有 URL，填充 cache，返回 {url: file_token}。

        设计原则：并发只发生在本方法内；调用方后续的串行逻辑（如 bak 处理）
        通过 cache 命中实现零改动加速。

        Args:
            url_list: 待解析的 URL 列表
            concurrency: 并发线程数，默认 4（飞书附件 API 限频 ~100 QPS，安全范围内）

        Returns:
            dict[str, str]: 成功解析的 {url: file_token} 映射
        """
        with self._cache_lock:
            pending = [u for u in url_list if u not in self._token_cache]

        if not pending:
            logger.info(
                "resolve_batch: all %d URLs already cached, nothing to do",
                len(url_list),
            )
            return dict(self._token_cache)

        self._stats_total = len(pending)
        self._stats_done = 0
        self._stats_failed = 0
        self._stats_start_time = time.monotonic()

        logger.info(
            "resolve_batch: %d URLs to resolve (%d already cached), concurrency=%d",
            len(pending),
            len(url_list) - len(pending),
            concurrency,
        )

        results: dict[str, str] = {}
        last_log_time = self._stats_start_time

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = {pool.submit(self.resolve_single, url): url for url in pending}
            for future in as_completed(futures):
                url = futures[future]
                try:
                    token = future.result()
                except Exception as exc:
                    logger.error(
                        "resolve_batch: unexpected error for url=%s: %s", url, exc
                    )
                    token = None

                self._stats_done += 1
                if token:
                    results[url] = token
                else:
                    self._stats_failed += 1

                # 进度日志：每 50 个或每 60 秒输出一次
                now = time.monotonic()
                should_log = (
                    self._stats_done % 50 == 0
                    or self._stats_done == self._stats_total
                    or (now - last_log_time) >= 60
                )
                if should_log:
                    elapsed = now - self._stats_start_time
                    rate = self._stats_done / max(elapsed, 1)
                    remaining = (self._stats_total - self._stats_done) / max(rate, 0.01)
                    logger.info(
                        "resolve_batch progress: %d/%d (%.1f%%) done, %d failed, "
                        "elapsed %.0fs, ETA %.0fs",
                        self._stats_done,
                        self._stats_total,
                        self._stats_done / max(self._stats_total, 1) * 100,
                        self._stats_failed,
                        elapsed,
                        remaining,
                    )
                    last_log_time = now

        elapsed = time.monotonic() - self._stats_start_time
        logger.info(
            "resolve_batch completed: %d resolved, %d failed, %.1f minutes",
            len(results),
            self._stats_failed,
            elapsed / 60,
        )
        return results

    def log_summary(self) -> None:
        """输出附件解析汇总统计。"""
        elapsed = (
            time.monotonic() - self._stats_start_time
            if self._stats_start_time > 0
            else 0
        )
        cache_size = len(self._token_cache)
        failed_size = len(self._failed_reason_cache)
        logger.info(
            "Attachment summary: cached=%d, failed=%d, "
            "batch_total=%d, batch_done=%d, batch_failed=%d, "
            "elapsed=%.1f min",
            cache_size,
            failed_size,
            self._stats_total,
            self._stats_done,
            self._stats_failed,
            elapsed / 60,
        )

    @staticmethod
    def _is_retryable(exc: Exception) -> bool:
        if isinstance(exc, RETRYABLE_EXCEPTIONS):
            return True
        text = str(exc).lower()
        return any(
            keyword in text
            for keyword in (
                "timeout",
                "temporar",
                "429",
                "503",
                "502",
                "504",
                "99991400",
                "frequency limit",
                "rate limit",
            )
        )
