# coding:utf8
from __future__ import annotations

import pandas as pd

from workers.upgrade_after_sale.main import (
    _apply_attachment_bak_columns,
    _build_date_params,
    _compute_window,
    _is_retryable_error,
    _pre_resolve_attachments,
)


def test_build_date_params_independent_offsets():
    as_params = _build_date_params("2026-06-26", -7, 0)
    od_params = _build_date_params("2026-06-26", -3, -1)

    as_sql = as_params.sql_params()
    od_sql = od_params.sql_params()

    assert as_sql["date_param"] == "DATE '2026-06-26'"
    assert as_sql["start_offset"] == "-7"
    assert as_sql["end_offset"] == "0"

    assert od_sql["date_param"] == "DATE '2026-06-26'"
    assert od_sql["start_offset"] == "-3"
    assert od_sql["end_offset"] == "-1"


def test_compute_window_from_params():
    params = _build_date_params("2026-06-26", -2, 1)
    start, end = _compute_window(params)
    assert str(start) == "2026-06-24"
    assert str(end) == "2026-06-27"


def test_retryable_error_patterns_cover_common_transient_errors():
    assert _is_retryable_error(RuntimeError("HTTP 504 gateway timeout"))
    assert _is_retryable_error(RuntimeError("1254291 write conflict"))
    assert not _is_retryable_error(RuntimeError("invalid request body"))


def test_apply_attachment_bak_columns_clears_attachment_and_keeps_raw():
    df = pd.DataFrame(
        {
            "客户申请举证视频": ["https://example.com/a.mp4", None],
            "其他字段": [1, 2],
        }
    )

    out = _apply_attachment_bak_columns(df)

    assert "客户申请举证视频_bak_raw" in out.columns
    assert out.loc[0, "客户申请举证视频_bak_raw"] == "https://example.com/a.mp4"
    assert out.loc[1, "客户申请举证视频_bak_raw"] == ""
    assert out.loc[0, "客户申请举证视频"] == "https://example.com/a.mp4"
    assert pd.isna(out.loc[1, "客户申请举证视频"])


class _FakeResolver:
    """Minimal fake resolver for _pre_resolve_attachments tests."""

    def __init__(self):
        self.batch_calls = []
        self.single_calls = []

    def resolve_batch(self, url_list, *, concurrency=4):
        self.batch_calls.append({"urls": list(url_list), "concurrency": concurrency})
        return {url: f"ft_{i}" for i, url in enumerate(url_list)}

    def resolve_single(self, url):
        self.single_calls.append(url)
        return f"ft_{url}"


def test_pre_resolve_attachments_collects_urls_and_calls_batch():
    df = pd.DataFrame(
        {
            "送达签收照片": [
                "https://example.com/a.jpg",
                "https://example.com/b.jpg",
            ],
            "客户申请举证图片": ["https://example.com/d.jpg", None],
            "客户申请举证视频": [
                '["https://example.com/e.mp4","https://example.com/f.mp4"]',
                "",
            ],
            "其他字段": [1, 2],
        }
    )
    resolver = _FakeResolver()
    _pre_resolve_attachments(
        df,
        ["送达签收照片", "客户申请举证图片", "客户申请举证视频"],
        resolver,
        concurrency=4,
    )

    assert len(resolver.batch_calls) == 1
    urls = resolver.batch_calls[0]["urls"]
    assert "https://example.com/a.jpg" in urls
    assert "https://example.com/b.jpg" in urls
    assert "https://example.com/d.jpg" in urls
    assert "https://example.com/e.mp4" in urls
    assert "https://example.com/f.mp4" in urls
    assert resolver.batch_calls[0]["concurrency"] == 4


def test_pre_resolve_attachments_empty_columns():
    df = pd.DataFrame({"其他字段": [1, 2]})
    resolver = _FakeResolver()
    _pre_resolve_attachments(df, ["送达签收照片"], resolver)
    # No attachment columns exist, so no batch call
    assert len(resolver.batch_calls) == 0


def test_pre_resolve_attachments_no_urls():
    df = pd.DataFrame(
        {
            "送达签收照片": [None, None],
        }
    )
    resolver = _FakeResolver()
    _pre_resolve_attachments(df, ["送达签收照片"], resolver)
    # All values are NaN, no URLs to resolve
    assert len(resolver.batch_calls) == 0
