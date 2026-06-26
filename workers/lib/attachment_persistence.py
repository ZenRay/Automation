# coding:utf8
"""Attachment persistence utilities.

This module stores processing artifacts in JSONL/JSON for:
- parse/upload/write events
- checkpoint stage progress
- retry from latest failed rows/urls
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as writer:
        json.dump(payload, writer, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as writer:
        writer.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as reader:
        for line in reader:
            text = line.strip()
            if not text:
                continue
            rows.append(json.loads(text))
    return rows


@dataclass
class AttachmentPersistence:
    artifact_dir: str
    job_id: str

    def __post_init__(self):
        self.base_dir = Path(self.artifact_dir) / "attachments" / self.job_id
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.input_snapshot_path = self.base_dir / "input_snapshot.jsonl"
        self.parsed_snapshot_path = self.base_dir / "parsed_snapshot.jsonl"
        self.upload_events_path = self.base_dir / "upload_events.jsonl"
        self.write_events_path = self.base_dir / "write_events.jsonl"
        self.checkpoint_path = self.base_dir / "checkpoint.json"

    def append_input_snapshot(self, *, target_name: str, row_key: str, field_name: str, raw_value: Any) -> None:
        _append_jsonl(
            self.input_snapshot_path,
            {
                "ts": _utc_now_iso(),
                "target_name": target_name,
                "row_key": row_key,
                "field_name": field_name,
                "raw_value": raw_value,
            },
        )

    def append_parsed_snapshot(
        self,
        *,
        target_name: str,
        row_key: str,
        field_name: str,
        normalized_urls: list[str],
        parse_status: str,
        parse_error: str | None = None,
    ) -> None:
        _append_jsonl(
            self.parsed_snapshot_path,
            {
                "ts": _utc_now_iso(),
                "target_name": target_name,
                "row_key": row_key,
                "field_name": field_name,
                "normalized_urls": normalized_urls,
                "parse_status": parse_status,
                "parse_error": parse_error,
            },
        )

    def append_upload_event(
        self,
        *,
        target_name: str,
        row_key: str,
        field_name: str,
        normalized_url: str,
        upload_status: str,
        retry_count: int,
        file_token: str | None = None,
        error_type: str | None = None,
        error_message: str | None = None,
        retryable: bool | None = None,
    ) -> None:
        _append_jsonl(
            self.upload_events_path,
            {
                "ts": _utc_now_iso(),
                "target_name": target_name,
                "row_key": row_key,
                "field_name": field_name,
                "normalized_url": normalized_url,
                "upload_status": upload_status,
                "retry_count": retry_count,
                "file_token": file_token,
                "error_type": error_type,
                "error_message": error_message,
                "retryable": retryable,
            },
        )

    def append_write_event(
        self,
        *,
        target_name: str,
        row_key: str,
        write_status: str,
        error_message: str | None = None,
    ) -> None:
        _append_jsonl(
            self.write_events_path,
            {
                "ts": _utc_now_iso(),
                "target_name": target_name,
                "row_key": row_key,
                "write_status": write_status,
                "error_message": error_message,
            },
        )

    def save_checkpoint(self, *, stage: str, batch_index: int = 0, counters: dict[str, int] | None = None) -> None:
        payload = {
            "ts": _utc_now_iso(),
            "stage": stage,
            "batch_index": batch_index,
            "counters": counters or {},
        }
        _atomic_write_json(self.checkpoint_path, payload)

    def load_checkpoint(self) -> dict[str, Any]:
        if not self.checkpoint_path.exists():
            return {"stage": "init", "batch_index": 0, "counters": {}}
        with self.checkpoint_path.open("r", encoding="utf-8") as reader:
            return json.load(reader)

    def load_latest_upload_token_map(self, *, target_name: str | None = None) -> dict[str, str]:
        events = _read_jsonl(self.upload_events_path)
        latest: dict[str, str] = {}
        for item in events:
            if target_name and item.get("target_name") != target_name:
                continue
            if item.get("upload_status") != "success":
                continue
            url = item.get("normalized_url")
            token = item.get("file_token")
            if isinstance(url, str) and isinstance(token, str) and url and token:
                latest[url] = token
        return latest

    def load_current_failed_upload_urls(self, *, target_name: str | None = None) -> list[dict[str, Any]]:
        events = _read_jsonl(self.upload_events_path)
        latest_by_key: dict[tuple[str, str, str, str], dict[str, Any]] = {}
        for item in events:
            if target_name and item.get("target_name") != target_name:
                continue
            key = (
                item.get("target_name", ""),
                item.get("row_key", ""),
                item.get("field_name", ""),
                item.get("normalized_url", ""),
            )
            latest_by_key[key] = item
        return [
            item
            for item in latest_by_key.values()
            if item.get("upload_status") == "failed" and item.get("retryable") is True
        ]

    def load_current_failed_write_rows(self, *, target_name: str | None = None) -> list[str]:
        events = _read_jsonl(self.write_events_path)
        latest_status: dict[tuple[str, str], str] = {}
        for item in events:
            if target_name and item.get("target_name") != target_name:
                continue
            key = (item.get("target_name", ""), item.get("row_key", ""))
            latest_status[key] = item.get("write_status", "")
        failed = [key[1] for key, status in latest_status.items() if status == "failed"]
        return failed

    def summarize_events(self) -> dict[str, int]:
        summary = defaultdict(int)
        for item in _read_jsonl(self.upload_events_path):
            summary[f"upload_{item.get('upload_status', 'unknown')}"] += 1
        for item in _read_jsonl(self.write_events_path):
            summary[f"write_{item.get('write_status', 'unknown')}"] += 1
        return dict(summary)
