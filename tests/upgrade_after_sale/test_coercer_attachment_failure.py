"""Tests for attachment failure row-level marking in FieldTypeCoercer + lark_loader.

Covers:
- _coerce_attachment marks row as failed when ALL URLs fail
- Partial success (some URLs succeed, some fail) does NOT mark row as failed
- No attachment_resolver: no failure tracking
- apply_to_dataframe resets _attachment_failed_rows on each call
- _write_records_batched writes "partial" for attachment-failed rows
- Integration: coercer → lark_loader → persistence marks correct write_status
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.type_coercer import FieldTypeCoercer
from workers.lib.models import FieldMapping, LarkFieldType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_attachment_mapping(
    source_col: str = "附件",
    target_field: str = "附件",
) -> FieldMapping:
    return FieldMapping(
        source_col=source_col,
        lark_type=LarkFieldType.ATTACHMENT,
        target_field=target_field,
    )


def _resolver_all_fail():
    """Returns a resolver callable that always returns None (all uploads fail)."""
    return lambda url: None


def _resolver_some_fail(fail_urls: set[str]):
    """Returns a resolver that fails for specific URLs, succeeds for others."""

    def resolver(url):
        if url in fail_urls:
            return None
        return {"file_token": f"tok_{url}"}

    return resolver


def _resolver_all_succeed():
    return lambda url: {"file_token": f"tok_{url}"}


# ---------------------------------------------------------------------------
# Tests for FieldTypeCoercer._coerce_attachment failure tracking
# ---------------------------------------------------------------------------


class TestCoerceAttachmentFailureTracking:
    def test_all_urls_fail_marks_row(self):
        """When all attachment URLs fail, row_idx is added to _attachment_failed_rows."""
        coercer = FieldTypeCoercer(attachment_resolver=_resolver_all_fail())
        # Use JSON-list format so normalize_attachment_input parses multiple URLs
        result = coercer._coerce_attachment(
            '["http://example.com/a.jpg","http://example.com/b.jpg"]',
            row_idx=0,
        )
        assert result == []
        assert coercer.attachment_failed_rows == frozenset({0})

    def test_partial_success_does_not_mark_row(self):
        """When some URLs succeed, row is NOT marked as failed."""
        resolver = _resolver_some_fail({"http://example.com/fail.jpg"})
        coercer = FieldTypeCoercer(attachment_resolver=resolver)
        result = coercer._coerce_attachment(
            '["http://example.com/ok.jpg","http://example.com/fail.jpg"]',
            row_idx=1,
        )
        assert result == [{"file_token": "tok_http://example.com/ok.jpg"}]
        assert coercer.attachment_failed_rows == frozenset()

    def test_all_urls_succeed_no_failure_mark(self):
        """When all URLs succeed, row is NOT marked as failed."""
        coercer = FieldTypeCoercer(attachment_resolver=_resolver_all_succeed())
        result = coercer._coerce_attachment(
            "http://example.com/a.jpg",
            row_idx=2,
        )
        assert len(result) == 1
        assert coercer.attachment_failed_rows == frozenset()

    def test_no_resolver_no_failure_tracking(self):
        """Without attachment_resolver, no failure is tracked."""
        coercer = FieldTypeCoercer(attachment_resolver=None)
        result = coercer._coerce_attachment(
            "http://example.com/a.jpg",
            row_idx=0,
        )
        # Without resolver, returns [{"url": ...}] format
        assert result == [{"url": "http://example.com/a.jpg"}]
        assert coercer.attachment_failed_rows == frozenset()

    def test_empty_value_no_failure(self):
        """Empty attachment value is not a failure."""
        coercer = FieldTypeCoercer(attachment_resolver=_resolver_all_fail())
        result = coercer._coerce_attachment("", row_idx=0)
        assert result == []
        assert coercer.attachment_failed_rows == frozenset()


# ---------------------------------------------------------------------------
# Tests for apply_to_dataframe integration
# ---------------------------------------------------------------------------


class TestApplyToDataframeAttachmentFailure:
    def test_tracks_multiple_failed_rows(self):
        """Multiple rows with all-attachment-failure are all tracked."""
        coercer = FieldTypeCoercer(attachment_resolver=_resolver_all_fail())
        df = pd.DataFrame(
            {
                "附件": [
                    "http://example.com/a.jpg",
                    "http://example.com/b.jpg",
                    "",  # empty — not a failure
                    "http://example.com/c.jpg",
                ],
            }
        )
        mappings = [_make_attachment_mapping()]
        records = coercer.apply_to_dataframe(df, mappings)

        assert len(records) == 4
        # Rows 0, 1, 3 have attachment URLs that all failed
        assert coercer.attachment_failed_rows == frozenset({0, 1, 3})

    def test_resets_on_each_call(self):
        """_attachment_failed_rows is cleared at the start of apply_to_dataframe."""
        coercer = FieldTypeCoercer(attachment_resolver=_resolver_all_fail())
        mappings = [_make_attachment_mapping()]

        # First call: row 0 fails
        df1 = pd.DataFrame({"附件": ["http://example.com/a.jpg"]})
        coercer.apply_to_dataframe(df1, mappings)
        assert coercer.attachment_failed_rows == frozenset({0})

        # Second call: row 0 succeeds (different resolver needed — recreate coercer)
        coercer2 = FieldTypeCoercer(attachment_resolver=_resolver_all_succeed())
        df2 = pd.DataFrame({"附件": ["http://example.com/b.jpg"]})
        coercer2.apply_to_dataframe(df2, mappings)
        assert coercer2.attachment_failed_rows == frozenset()

    def test_coerce_for_write_passes_row_idx(self):
        """coerce_for_write passes row_idx to _coerce_attachment for ATTACHMENT type."""
        coercer = FieldTypeCoercer(attachment_resolver=_resolver_all_fail())
        result = coercer.coerce_for_write(
            "http://example.com/a.jpg",
            lark_type=LarkFieldType.ATTACHMENT,
            row_idx=5,
        )
        assert result == []
        assert coercer.attachment_failed_rows == frozenset({5})

    def test_coerce_for_write_without_row_idx(self):
        """coerce_for_write without row_idx doesn't track failures (backward compat)."""
        coercer = FieldTypeCoercer(attachment_resolver=_resolver_all_fail())
        result = coercer.coerce_for_write(
            "http://example.com/a.jpg",
            lark_type=LarkFieldType.ATTACHMENT,
        )
        assert result == []
        # No row_idx → no tracking
        assert coercer.attachment_failed_rows == frozenset()


# ---------------------------------------------------------------------------
# Tests for _write_records_batched partial marking
# ---------------------------------------------------------------------------


class TestWriteRecordsBatchedPartial:
    def test_attachment_failed_rows_written_as_partial(self):
        """Rows in attachment_failed_row_keys get write_status='partial'."""
        from workers.lib.lark_loader import _write_records_batched

        client = MagicMock()
        persistence = MagicMock()

        records = [
            {"fields": {"name": "row1"}},
            {"fields": {"name": "row2"}},
            {"fields": {"name": "row3"}},
        ]
        row_keys = ["key_1", "key_2", "key_3"]

        _write_records_batched(
            client=client,
            table_id="tbl123",
            target_name="test_target",
            records=records,
            persistence=persistence,
            row_keys=row_keys,
            attachment_failed_row_keys={"key_2"},
        )

        # Check persistence calls: key_1=success, key_2=partial, key_3=success
        write_calls = persistence.append_write_event.call_args_list
        assert len(write_calls) == 3
        assert write_calls[0].kwargs["row_key"] == "key_1"
        assert write_calls[0].kwargs["write_status"] == "success"
        assert write_calls[1].kwargs["row_key"] == "key_2"
        assert write_calls[1].kwargs["write_status"] == "partial"
        assert write_calls[2].kwargs["row_key"] == "key_3"
        assert write_calls[2].kwargs["write_status"] == "success"

    def test_no_attachment_failed_keys_all_success(self):
        """When attachment_failed_row_keys is None, all rows are 'success'."""
        from workers.lib.lark_loader import _write_records_batched

        client = MagicMock()
        persistence = MagicMock()

        records = [{"fields": {"name": "row1"}}]
        row_keys = ["key_1"]

        _write_records_batched(
            client=client,
            table_id="tbl123",
            target_name="test_target",
            records=records,
            persistence=persistence,
            row_keys=row_keys,
            attachment_failed_row_keys=None,
        )

        write_calls = persistence.append_write_event.call_args_list
        assert len(write_calls) == 1
        assert write_calls[0].kwargs["write_status"] == "success"

    def test_multiple_partial_rows(self):
        """Multiple rows marked as partial."""
        from workers.lib.lark_loader import _write_records_batched

        client = MagicMock()
        persistence = MagicMock()

        records = [{"fields": {"name": f"row{i}"}} for i in range(5)]
        row_keys = [f"key_{i}" for i in range(5)]

        _write_records_batched(
            client=client,
            table_id="tbl123",
            target_name="test_target",
            records=records,
            persistence=persistence,
            row_keys=row_keys,
            attachment_failed_row_keys={"key_1", "key_3"},
        )

        write_calls = persistence.append_write_event.call_args_list
        statuses = [c.kwargs["write_status"] for c in write_calls]
        assert statuses == ["success", "partial", "success", "partial", "success"]
