# coding:utf8
"""tests/upgrade_after_sale/test_attachment_persistence_inject.py

验证 _build_attachment_resolver 的 persistence 注入机制：
1. 构造时注入 persistence 对象，resolve_single 成功时调用 append_upload_event(status=success)
2. resolve_single 失败时调用 append_upload_event(status=failed)
3. 未注入 persistence 时（默认），resolve_single 正常运行不报错
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from workers.lib.attachment_token_resolver import AttachmentTokenResolver
from workers.upgrade_after_sale.main import _build_attachment_resolver


class TestBuildAttachmentResolverPersistence:
    """_build_attachment_resolver 的 persistence 参数注入测试。"""

    def test_persistence_defaults_to_none(self):
        """默认情况下 persistence 应为 None（向后兼容）。"""
        client = MagicMock()
        resolver = _build_attachment_resolver(client)
        assert resolver.persistence is None

    def test_persistence_is_injected(self):
        """传入 persistence 后 resolver.persistence 应被设置。"""
        client = MagicMock()
        persistence = MagicMock()
        resolver = _build_attachment_resolver(client, persistence=persistence)
        assert resolver.persistence is persistence


class TestResolveSingleWithPersistence:
    """resolve_single 在 persistence 注入后的行为验证。"""

    @pytest.fixture
    def mock_persistence(self):
        """构建 mock RouteWritePersistence。"""
        return MagicMock()

    @pytest.fixture
    def resolver_with_persistence(self, mock_persistence):
        """构建带 persistence 的 resolver。"""
        client = MagicMock()
        client.app_token = "test_app_token"
        resolver = AttachmentTokenResolver(
            client=client,
            app_token="test_app_token",
            max_retries=0,
            backoff_seconds=0.1,
            persistence=mock_persistence,
            target_name="test_target",
            field_name="test_field",
        )
        return resolver

    def test_success_calls_append_upload_event_with_success(
        self, resolver_with_persistence, mock_persistence
    ):
        """成功上传时调用 append_upload_event(status=success)。"""
        mock_response = {"data": {"file_token": "ft_test_123"}}
        resolver_with_persistence.client.upload_attachment.return_value = mock_response

        with patch(
            "workers.lib.attachment_token_resolver.download_url_to_tempfile"
        ) as mock_dl:
            mock_dl.return_value = ("/tmp/test.jpg", "image/jpeg")
            with patch(
                "workers.lib.attachment_token_resolver.safe_remove_file"
            ):
                result = resolver_with_persistence.resolve_single(
                    "https://example.com/test.jpg", row_key="row_1"
                )

        assert result == "ft_test_123"
        # 验证持久化调用
        mock_persistence.append_upload_event.assert_called_once()
        call_kwargs = mock_persistence.append_upload_event.call_args[1]
        assert call_kwargs["upload_status"] == "success"
        assert call_kwargs["file_token"] == "ft_test_123"
        assert call_kwargs["normalized_url"] == "https://example.com/test.jpg"
        assert call_kwargs["row_key"] == "row_1"

    def test_failure_calls_append_upload_event_with_failed(
        self, resolver_with_persistence, mock_persistence
    ):
        """上传失败时调用 append_upload_event(status=failed)。"""
        resolver_with_persistence.client.upload_attachment.side_effect = (
            ConnectionError("Remote end closed connection")
        )

        with patch(
            "workers.lib.attachment_token_resolver.download_url_to_tempfile"
        ) as mock_dl:
            mock_dl.return_value = ("/tmp/test.jpg", "image/jpeg")
            with patch(
                "workers.lib.attachment_token_resolver.safe_remove_file"
            ):
                result = resolver_with_persistence.resolve_single(
                    "https://example.com/fail.jpg", row_key="row_2"
                )

        assert result is None
        # 验证持久化调用
        mock_persistence.append_upload_event.assert_called()
        last_call = mock_persistence.append_upload_event.call_args[1]
        assert last_call["upload_status"] == "failed"
        assert last_call["error_type"] == "ConnectionError"
        assert "Remote end closed connection" in last_call["error_message"]

    def test_no_persistence_does_not_raise(self):
        """未注入 persistence 时 resolve_single 正常运行不报错。"""
        client = MagicMock()
        client.app_token = "test_app_token"
        resolver = AttachmentTokenResolver(
            client=client,
            app_token="test_app_token",
            max_retries=0,
            backoff_seconds=0.1,
            persistence=None,  # 显式 None
        )

        mock_response = {"data": {"file_token": "ft_no_persist"}}
        client.upload_attachment.return_value = mock_response

        with patch(
            "workers.lib.attachment_token_resolver.download_url_to_tempfile"
        ) as mock_dl:
            mock_dl.return_value = ("/tmp/test.jpg", "image/jpeg")
            with patch(
                "workers.lib.attachment_token_resolver.safe_remove_file"
            ):
                result = resolver.resolve_single("https://example.com/ok.jpg")

        assert result == "ft_no_persist"


class TestResolveInputSnapshotWithPersistence:
    """resolve() 方法中 input_snapshot 和 parsed_snapshot 持久化验证。"""

    def test_resolve_calls_input_and_parsed_snapshot(self):
        """resolve() 应调用 append_input_snapshot 和 append_parsed_snapshot。"""
        client = MagicMock()
        client.app_token = "test_app_token"
        persistence = MagicMock()
        resolver = AttachmentTokenResolver(
            client=client,
            app_token="test_app_token",
            max_retries=0,
            persistence=persistence,
            target_name="test_target",
            field_name="附件字段",
        )

        # resolve_single 返回 None（上传失败），避免完整上传流程
        resolver.resolve_single = MagicMock(return_value=None)

        resolver.resolve("https://example.com/photo.jpg")

        persistence.append_input_snapshot.assert_called_once()
        input_kwargs = persistence.append_input_snapshot.call_args[1]
        assert input_kwargs["target_name"] == "test_target"
        assert input_kwargs["field_name"] == "附件字段"

        persistence.append_parsed_snapshot.assert_called_once()
        parsed_kwargs = persistence.append_parsed_snapshot.call_args[1]
        assert parsed_kwargs["parse_status"] == "success"
        assert len(parsed_kwargs["normalized_urls"]) == 1
