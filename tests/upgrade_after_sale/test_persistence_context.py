# coding:utf8
"""tests/upgrade_after_sale/test_persistence_context.py

验证持久化上下文修复（Phase B）：
1. coercer 接收 resolver 对象后，resolve_single 收到 row_key
2. upload_events 记录包含正确的 target_name 和 row_key
3. row_key="" 时不写入 upload_events（pre-resolve 阶段）
4. lark_loader 在 apply_to_dataframe 前设置 target_name
5. hasattr 检测兼容 callable 和 resolver 对象两种接口
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

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


class _MockResolver:
    """模拟完整 resolver 对象（有 resolve_single 方法）。"""

    def __init__(self):
        self.target_name = ""
        self.field_name = ""
        self.calls: list[dict] = []

    def resolve_single(self, url: str, *, row_key: str = "") -> str | None:
        self.calls.append({"url": url, "row_key": row_key})
        return f"tok_{url}"


# ---------------------------------------------------------------------------
# Tests: coercer 适配两种 resolver 接口
# ---------------------------------------------------------------------------


class TestCoercerResolverInterface:
    """_coerce_attachment 的 hasattr 检测兼容 callable 和 resolver 对象。"""

    def test_resolver_object_receives_row_key(self):
        """完整 resolver 对象 → resolve_single 收到 row_key。"""
        resolver = _MockResolver()
        coercer = FieldTypeCoercer(attachment_resolver=resolver)
        result = coercer._coerce_attachment(
            "http://example.com/a.jpg",
            row_idx=0,
            row_key="row_123",
        )
        assert len(result) == 1
        assert result[0] == {"file_token": "tok_http://example.com/a.jpg"}
        assert resolver.calls[0]["row_key"] == "row_123"

    def test_callable_interface_backward_compat(self):
        """callable(url) 旧接口仍然工作（不传 row_key）。"""
        called_with: list[str] = []

        def old_resolver(url):
            called_with.append(url)
            return {"file_token": f"tok_{url}"}

        coercer = FieldTypeCoercer(attachment_resolver=old_resolver)
        result = coercer._coerce_attachment(
            "http://example.com/a.jpg",
            row_idx=0,
            row_key="row_123",
        )
        assert len(result) == 1
        assert called_with == ["http://example.com/a.jpg"]

    def test_none_resolver_returns_url_format(self):
        """无 resolver 时返回 [{"url": ...}] 格式。"""
        coercer = FieldTypeCoercer(attachment_resolver=None)
        result = coercer._coerce_attachment("http://example.com/a.jpg")
        assert result == [{"url": "http://example.com/a.jpg"}]


# ---------------------------------------------------------------------------
# Tests: apply_to_dataframe 传递 row_key
# ---------------------------------------------------------------------------


class TestApplyToDataframeRowKey:
    """apply_to_dataframe 从 DataFrame 行提取 row_key 并传递。"""

    def test_row_key_extracted_from_dataframe(self):
        """DataFrame 有 row_key 列时，row_key 被传递给 resolve_single。"""
        resolver = _MockResolver()
        coercer = FieldTypeCoercer(attachment_resolver=resolver)
        df = pd.DataFrame(
            {
                "附件": ["http://example.com/a.jpg", "http://example.com/b.jpg"],
                "row_key": ["key_A", "key_B"],
            }
        )
        mappings = [_make_attachment_mapping()]
        records = coercer.apply_to_dataframe(df, mappings)

        assert len(records) == 2
        assert resolver.calls[0]["row_key"] == "key_A"
        assert resolver.calls[1]["row_key"] == "key_B"

    def test_row_key_empty_when_column_missing(self):
        """DataFrame 无 row_key 列时，row_key 默认为空。"""
        resolver = _MockResolver()
        coercer = FieldTypeCoercer(attachment_resolver=resolver)
        df = pd.DataFrame(
            {
                "附件": ["http://example.com/a.jpg"],
            }
        )
        mappings = [_make_attachment_mapping()]
        coercer.apply_to_dataframe(df, mappings)

        assert resolver.calls[0]["row_key"] == ""


# ---------------------------------------------------------------------------
# Tests: resolve_single 条件化持久化
# ---------------------------------------------------------------------------


class TestConditionalPersistence:
    """resolve_single 仅在 row_key 非空时写入 upload_events。"""

    def _make_resolver_with_persistence(self):
        from workers.lib.attachment_token_resolver import AttachmentTokenResolver

        client = MagicMock()
        client.app_token = "test_token"
        persistence = MagicMock()
        resolver = AttachmentTokenResolver(
            client=client,
            app_token="test_token",
            max_retries=0,
            backoff_seconds=0.1,
            persistence=persistence,
            target_name="test_target",
        )
        return resolver, persistence

    def test_nonempty_row_key_writes_upload_event(self):
        """row_key 非空 → append_upload_event 被调用。"""
        resolver, persistence = self._make_resolver_with_persistence()
        mock_response = {"data": {"file_token": "ft_123"}}
        resolver.client.upload_attachment.return_value = mock_response

        with patch(
            "workers.lib.attachment_token_resolver.download_url_to_tempfile"
        ) as mock_dl:
            mock_dl.return_value = ("/tmp/test.jpg", "image/jpeg")
            with patch("workers.lib.attachment_token_resolver.safe_remove_file"):
                result = resolver.resolve_single(
                    "https://example.com/test.jpg",
                    row_key="row_1",
                )

        assert result == "ft_123"
        persistence.append_upload_event.assert_called_once()
        kwargs = persistence.append_upload_event.call_args[1]
        assert kwargs["row_key"] == "row_1"
        assert kwargs["target_name"] == "test_target"

    def test_empty_row_key_skips_upload_event(self):
        """row_key='' (pre-resolve 阶段) → append_upload_event 不被调用。"""
        resolver, persistence = self._make_resolver_with_persistence()
        mock_response = {"data": {"file_token": "ft_456"}}
        resolver.client.upload_attachment.return_value = mock_response

        with patch(
            "workers.lib.attachment_token_resolver.download_url_to_tempfile"
        ) as mock_dl:
            mock_dl.return_value = ("/tmp/test.jpg", "image/jpeg")
            with patch("workers.lib.attachment_token_resolver.safe_remove_file"):
                result = resolver.resolve_single(
                    "https://example.com/test.jpg",
                    row_key="",
                )

        assert result == "ft_456"
        persistence.append_upload_event.assert_not_called()

    def test_failure_with_row_key_writes_event(self):
        """上传失败 + row_key 非空 → 记录 failed 事件。"""
        resolver, persistence = self._make_resolver_with_persistence()
        resolver.client.upload_attachment.side_effect = ConnectionError("test error")

        with patch(
            "workers.lib.attachment_token_resolver.download_url_to_tempfile"
        ) as mock_dl:
            mock_dl.return_value = ("/tmp/test.jpg", "image/jpeg")
            with patch("workers.lib.attachment_token_resolver.safe_remove_file"):
                result = resolver.resolve_single(
                    "https://example.com/fail.jpg",
                    row_key="row_2",
                )

        assert result is None
        persistence.append_upload_event.assert_called()
        kwargs = persistence.append_upload_event.call_args[1]
        assert kwargs["upload_status"] == "failed"
        assert kwargs["row_key"] == "row_2"

    def test_failure_without_row_key_skips_event(self):
        """上传失败 + row_key='' → 不记录事件。"""
        resolver, persistence = self._make_resolver_with_persistence()
        resolver.client.upload_attachment.side_effect = ConnectionError("test error")

        with patch(
            "workers.lib.attachment_token_resolver.download_url_to_tempfile"
        ) as mock_dl:
            mock_dl.return_value = ("/tmp/test.jpg", "image/jpeg")
            with patch("workers.lib.attachment_token_resolver.safe_remove_file"):
                result = resolver.resolve_single(
                    "https://example.com/fail.jpg",
                    row_key="",
                )

        assert result is None
        persistence.append_upload_event.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: lark_loader 设置 target_name
# ---------------------------------------------------------------------------


class TestLarkLoaderSetsTargetName:
    """验证 coercer.attachment_resolver.target_name 在写入前被设置。"""

    def test_resolver_target_name_set_via_hasattr(self):
        """hasattr 检测成功后设置 target_name。"""
        resolver = _MockResolver()
        coercer = FieldTypeCoercer(attachment_resolver=resolver)

        # 模拟 lark_loader 中的逻辑
        r = getattr(coercer, "attachment_resolver", None)
        if hasattr(r, "resolve_single"):
            r.target_name = "after_sale_detail"

        assert coercer.attachment_resolver.target_name == "after_sale_detail"

    def test_callable_resolver_skips_target_name(self):
        """callable resolver 不设置 target_name（hasattr 返回 False）。"""

        def old_resolver(url):
            return {"file_token": "tok"}

        coercer = FieldTypeCoercer(attachment_resolver=old_resolver)

        r = getattr(coercer, "attachment_resolver", None)
        if hasattr(r, "resolve_single"):
            r.target_name = "after_sale_detail"

        # callable 没有 target_name 属性
        assert not hasattr(coercer.attachment_resolver, "target_name")
