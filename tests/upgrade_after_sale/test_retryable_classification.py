"""Tests for _is_retryable and _is_permanent_failure error classification.

Covers:
- requests.exceptions.ConnectionError wrapping RemoteDisconnected → retryable
- "Connection aborted." message → retryable
- "Max retries exceeded" (SSL) → retryable
- HTTPError 404 → not retryable (permanent)
- HTTPError 502 → retryable
- Rate limit (99991400) → retryable
- Built-in ConnectionError → retryable via isinstance
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import requests.exceptions

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.attachment_token_resolver import AttachmentTokenResolver

_is_retryable = AttachmentTokenResolver._is_retryable
_is_permanent_failure = AttachmentTokenResolver._is_permanent_failure


# ---------------------------------------------------------------------------
# Permanent failure tests
# ---------------------------------------------------------------------------


class TestPermanentFailure:
    def test_size_limit(self):
        assert _is_permanent_failure(Exception("Attachment exceeds size limit: 20 MB"))

    def test_404_not_found(self):
        assert _is_permanent_failure(Exception("404 Client Error: Not Found"))

    def test_forbidden(self):
        assert _is_permanent_failure(Exception("403 Forbidden"))

    def test_connection_aborted_is_not_permanent(self):
        """Connection aborted is transient, NOT permanent."""
        assert not _is_permanent_failure(
            Exception("('Connection aborted.', RemoteDisconnected('...'))")
        )


# ---------------------------------------------------------------------------
# Retryable classification tests
# ---------------------------------------------------------------------------


class TestRetryableClassification:
    """Verify that transient errors are retryable and permanent ones are not."""

    # --- Transient network errors (requests.exceptions.ConnectionError) ---

    def test_requests_connection_error_remote_disconnected(self):
        """requests.exceptions.ConnectionError wrapping RemoteDisconnected → retryable."""
        inner = requests.exceptions.ConnectionError(
            "Connection aborted.", ConnectionResetError("Connection aborted")
        )
        assert _is_retryable(inner)

    def test_connection_aborted_message(self):
        """'Connection aborted.' in message → retryable."""
        exc = Exception("('Connection aborted.', RemoteDisconnected('Remote end closed'))")
        assert _is_retryable(exc)

    def test_connection_reset_message(self):
        """'Connection reset by peer' → retryable."""
        exc = Exception("Connection reset by peer")
        assert _is_retryable(exc)

    def test_remote_end_closed_message(self):
        """'Remote end closed connection without response' → retryable."""
        exc = Exception("Remote end closed connection without response")
        assert _is_retryable(exc)

    def test_ssl_max_retries_exceeded(self):
        """SSL 'Max retries exceeded' → retryable."""
        exc = Exception(
            "HTTPSConnectionPool(host='open.feishu.cn', port=443): "
            "Max retries exceeded with url: /open-apis/drive/v1/medias/upload_all"
        )
        assert _is_retryable(exc)

    def test_broken_pipe(self):
        """'Broken pipe' → retryable."""
        assert _is_retryable(Exception("Broken pipe"))

    def test_eof_occurred(self):
        """'EOF occurred' → retryable."""
        assert _is_retryable(Exception("EOF occurred unexpectedly"))

    # --- Rate limit / server errors ---

    def test_rate_limit_99991400(self):
        assert _is_retryable(Exception("API error code=99991400"))

    def test_http_502_bad_gateway(self):
        assert _is_retryable(Exception("502 Server Error: Bad Gateway"))

    def test_http_503_service_unavailable(self):
        assert _is_retryable(Exception("503 Service Unavailable"))

    def test_http_504_gateway_timeout(self):
        assert _is_retryable(Exception("504 Gateway Timeout"))

    def test_http_429_too_many_requests(self):
        assert _is_retryable(Exception("429 Too Many Requests"))

    # --- Built-in ConnectionError (isinstance path) ---

    def test_builtin_connection_error(self):
        """Built-in ConnectionError → retryable via isinstance."""
        assert _is_retryable(ConnectionError("network unreachable"))

    def test_builtin_timeout_error(self):
        assert _is_retryable(TimeoutError("timed out"))

    # --- Permanent errors (NOT retryable) ---

    def test_http_404_not_retryable(self):
        exc = Exception("404 Client Error: Not Found for url: https://example.com")
        assert not _is_retryable(exc)

    def test_http_403_not_retryable(self):
        assert not _is_retryable(Exception("403 Forbidden"))

    def test_size_exceeds_not_retryable(self):
        assert not _is_retryable(Exception("Attachment exceeds size limit: 20 MB"))

    def test_unsupported_url_not_retryable(self):
        assert not _is_retryable(Exception("Unsupported attachment URL scheme"))

    def test_generic_error_not_retryable(self):
        """Generic unknown error without retryable keywords → not retryable."""
        assert not _is_retryable(Exception("Something completely unexpected happened"))


# ---------------------------------------------------------------------------
# Regression test: actual production error patterns
# ---------------------------------------------------------------------------


class TestProductionErrorPatterns:
    """Regression tests using actual error messages from production logs."""

    def test_production_remote_disconnected_pattern(self):
        """Exact error message from 2026-07-08 upload_events.jsonl (394 occurrences)."""
        # Simulate requests.exceptions.ConnectionError wrapping RemoteDisconnected
        from http.client import RemoteDisconnected
        inner = RemoteDisconnected("Remote end closed connection without response")
        exc = requests.exceptions.ConnectionError("Connection aborted.", inner)
        assert _is_retryable(exc), (
            "RemoteDisconnected wrapped in requests.ConnectionError should be retryable. "
            f"isinstance check: {isinstance(exc, ConnectionError)}, "
            f"str: {str(exc)}"
        )

    def test_production_ssl_error_pattern(self):
        """SSL error pattern from production logs."""
        exc = Exception(
            "HTTPSConnectionPool(host='open.feishu.cn', port=443): "
            "Max retries exceeded with url: /open-apis/drive/v1/medias/upload_all "
            "(Caused by SSLError(SSLError('bad handshake')))"
        )
        assert _is_retryable(exc)

    def test_production_502_pattern(self):
        """502 Bad Gateway from production logs."""
        exc = Exception(
            "502 Server Error: Bad Gateway for url: "
            "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all"
        )
        assert _is_retryable(exc)

    def test_production_404_pattern(self):
        """404 from production logs should NOT be retryable."""
        exc = Exception(
            "404 Client Error: Not Found for url: "
            "https://ugc-pro.biaoguoworks.com/tms/persimmon/..."
        )
        assert not _is_retryable(exc)
