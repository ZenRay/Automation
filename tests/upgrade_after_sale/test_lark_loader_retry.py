# coding:utf8
"""tests/upgrade_after_sale/test_lark_loader_retry.py

验证 retry_failed_only 模式下 lark_loader 的行为：
1. 有失败行时跳过 cleanup，仅追加写入失败行
2. 无失败行时跳过 cleanup 和 write
3. 非 retry 模式时 cleanup 正常执行
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from workers.lib.lark_loader import _write_single_target
from workers.lib.models import CleanupCondition, LarkFieldType, FieldMapping


def _make_target(name="test_target"):
    """构建最小化 LarkTargetConfig 用于测试。"""
    from workers.lib.models import LarkTargetConfig

    return LarkTargetConfig(
        name=name,
        url="https://example.com/base/abc?table=tbl123&view=vew456",
        table_name="测试表",
        field_mappings=[
            FieldMapping(source_col="id", target_field="id", lark_type=LarkFieldType.TEXT),
            FieldMapping(source_col="value", target_field="value", lark_type=LarkFieldType.NUMBER),
        ],
        cleanup_conditions=CleanupCondition.date_window(
            "日期", pd.Timestamp("2026-07-01").date(), pd.Timestamp("2026-07-09").date()
        ),
    )


def _make_persistence_config(retry_failed_only=False, enabled=True):
    """构建最小化 PersistenceConfig。"""
    from types import SimpleNamespace

    return SimpleNamespace(
        enabled=enabled,
        artifact_dir="/tmp/test_persistence",
        job_id="test-job",
        retry_failed_only=retry_failed_only,
    )


def _make_df():
    """构建测试 DataFrame。"""
    return pd.DataFrame(
        {
            "id": ["row1", "row2", "row3"],
            "value": [1.0, 2.0, 3.0],
            "row_key": ["row1", "row2", "row3"],
        }
    )


def _setup_mocks():
    """设置通用的 mock 对象。"""
    client = MagicMock()
    client.app_token = "test_app_token"
    client.extract_app_information.return_value = None
    client.extract_table_information.return_value = {"测试表": "tbl_test"}

    persistence = MagicMock()
    persistence.load_checkpoint.return_value = {}
    persistence.load_latest_upload_token_map.return_value = {}

    return client, persistence


@pytest.fixture
def mock_persistence_module():
    """Mock RouteWritePersistence 构造函数，返回可控的 mock 对象。"""
    with patch("workers.lib.lark_loader.RouteWritePersistence") as mock_cls:
        yield mock_cls


@pytest.fixture
def mock_cleanup():
    """Mock cleanup_target_table 函数。"""
    with patch("workers.lib.lark_loader.cleanup_target_table") as mock_fn:
        mock_fn.return_value = 100
        yield mock_fn


@pytest.fixture
def mock_write_batched():
    """Mock _write_records_batched 函数。"""
    with patch("workers.lib.lark_loader._write_records_batched") as mock_fn:
        mock_fn.return_value = 3
        yield mock_fn


@pytest.fixture
def mock_ensure_fields():
    """Mock _ensure_target_fields 函数。"""
    with patch("workers.lib.lark_loader._ensure_target_fields") as mock_fn:
        yield mock_fn


class TestRetryFailedOnlyWithFailedRows:
    """retry_failed_only=True + 有失败行 -> 跳过 cleanup，仅写入失败行。"""

    def test_cleanup_is_skipped(
        self,
        mock_persistence_module,
        mock_cleanup,
        mock_write_batched,
        mock_ensure_fields,
    ):
        """有失败行时 cleanup 不应被调用。"""
        client, persistence = _setup_mocks()
        mock_persistence_module.return_value = persistence
        persistence.load_current_failed_write_rows.return_value = [
            "row1",
            "row3",
        ]

        _write_single_target(
            client=client,
            target=_make_target(),
            result_df=_make_df(),
            persistence_config=_make_persistence_config(retry_failed_only=True),
        )

        mock_cleanup.assert_not_called()

    def test_only_failed_rows_are_written(
        self,
        mock_persistence_module,
        mock_cleanup,
        mock_write_batched,
        mock_ensure_fields,
    ):
        """仅失败行应被写入。"""
        client, persistence = _setup_mocks()
        mock_persistence_module.return_value = persistence
        persistence.load_current_failed_write_rows.return_value = [
            "row1",
            "row3",
        ]

        _write_single_target(
            client=client,
            target=_make_target(),
            result_df=_make_df(),
            persistence_config=_make_persistence_config(retry_failed_only=True),
        )

        mock_write_batched.assert_called_once()
        call_kwargs = mock_write_batched.call_args
        records = call_kwargs[1].get("records") or call_kwargs[0][3]
        # 只有 row1 和 row3 应被写入
        assert len(records) == 2


class TestRetryFailedOnlyNoFailedRows:
    """retry_failed_only=True + 无失败行 -> 跳过 cleanup 和 write。"""

    def test_returns_zero_and_skips_all(
        self,
        mock_persistence_module,
        mock_cleanup,
        mock_write_batched,
        mock_ensure_fields,
    ):
        """无失败行时应返回 0，cleanup 和 write 都不执行。"""
        client, persistence = _setup_mocks()
        mock_persistence_module.return_value = persistence
        persistence.load_current_failed_write_rows.return_value = []

        result = _write_single_target(
            client=client,
            target=_make_target(),
            result_df=_make_df(),
            persistence_config=_make_persistence_config(retry_failed_only=True),
        )

        assert result == 0
        mock_cleanup.assert_not_called()
        mock_write_batched.assert_not_called()


class TestNormalModeCleanup:
    """retry_failed_only=False -> cleanup 正常执行。"""

    def test_cleanup_is_executed(
        self,
        mock_persistence_module,
        mock_cleanup,
        mock_write_batched,
        mock_ensure_fields,
    ):
        """非 retry 模式时 cleanup 应被调用。"""
        client, persistence = _setup_mocks()
        mock_persistence_module.return_value = persistence

        _write_single_target(
            client=client,
            target=_make_target(),
            result_df=_make_df(),
            persistence_config=_make_persistence_config(retry_failed_only=False),
        )

        mock_cleanup.assert_called_once()
