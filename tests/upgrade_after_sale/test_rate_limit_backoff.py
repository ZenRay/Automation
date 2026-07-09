"""Tests for rate-limit exponential backoff in AttachmentTokenResolver.

Covers:
- _is_rate_limited detection (99991400, "rate limit", "frequency limit")
- Rate limit errors use exponential backoff (2^n + jitter)
- Rate limit retries do NOT consume max_retries quota
- After MAX_RATE_LIMIT_RETRIES (5) the resolver gives up
- Normal (non-rate-limit) errors still use original backoff
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from workers.lib.attachment_token_resolver import (
    MAX_RATE_LIMIT_RETRIES,
    AttachmentTokenResolver,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_resolver(
    *,
    max_retries: int = 2,
    backoff_seconds: float = 0.4,
) -> AttachmentTokenResolver:
    client = MagicMock()
    return AttachmentTokenResolver(
        client=client,
        app_token="test_app_token",
        max_retries=max_retries,
        backoff_seconds=backoff_seconds,
    )


class RateLimitError(Exception):
    """Simulates Feishu rate-limit error (code=99991400)."""

    def __init__(self, msg: str = "API error: code=99991400 rate limit exceeded"):
        super().__init__(msg)


class NormalRetryableError(ConnectionError):
    """Simulates a normal retryable error (ConnectionError is in RETRYABLE_EXCEPTIONS)."""
    pass


class PermanentError(Exception):
    """Simulates a non-retryable error (e.g. file too large)."""

    def __init__(self, msg: str = "File size exceeds 20MB limit"):
        super().__init__(msg)


# ---------------------------------------------------------------------------
# Tests for _is_rate_limited
# ---------------------------------------------------------------------------


class TestIsRateLimited:
    def test_detects_99991400(self):
        assert AttachmentTokenResolver._is_rate_limited(
            Exception("code=99991400 too many requests")
        )

    def test_detects_rate_limit_keyword(self):
        assert AttachmentTokenResolver._is_rate_limited(
            Exception("Rate Limit exceeded")
        )

    def test_detects_frequency_limit_keyword(self):
        assert AttachmentTokenResolver._is_rate_limited(
            Exception("Frequency limit reached")
        )

    def test_ignores_unrelated_error(self):
        assert not AttachmentTokenResolver._is_rate_limited(
            Exception("Connection timeout")
        )


# ---------------------------------------------------------------------------
# Tests for resolve_single backoff behaviour
# ---------------------------------------------------------------------------


@patch("workers.lib.attachment_token_resolver.safe_remove_file")
@patch("workers.lib.attachment_token_resolver.guess_media_type", return_value="image")
@patch("workers.lib.attachment_token_resolver.normalize_filename", return_value="file.jpg")
@patch("workers.lib.attachment_token_resolver.download_url_to_tempfile",
       return_value=("/tmp/fake.jpg", "image/jpeg"))
class TestResolveSingleBackoff:
    """Rate-limit vs normal retry behaviour in resolve_single."""

    def _setup_upload_side_effect(
        self,
        resolver: AttachmentTokenResolver,
        side_effect,
    ):
        resolver.client.upload_attachment.side_effect = side_effect

    # --- Rate limit tests ---------------------------------------------------

    def test_rate_limit_uses_exponential_backoff(
        self, _dl, _fn, _gmt, _rm,
    ):
        """Sleep times for rate limit should be 2^n + jitter (not 0.4*2^n)."""
        resolver = _make_resolver(max_retries=2, backoff_seconds=0.4)

        # Fail 3 times with rate limit, then succeed
        call_count = {"n": 0}

        def upload_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] <= 3:
                raise RateLimitError()
            return {"file_token": "token_ok"}

        self._setup_upload_side_effect(resolver, upload_side_effect)

        sleep_times = []
        with patch("workers.lib.attachment_token_resolver.time.sleep",
                   side_effect=lambda s: sleep_times.append(s)):
            with patch("workers.lib.attachment_token_resolver.random.uniform",
                       return_value=0.0):  # no jitter for predictable test
                result = resolver.resolve_single("http://example.com/img.jpg")

        assert result == "token_ok"
        # 3 rate limit failures → sleep_times has 3 entries
        assert len(sleep_times) == 3
        # Exponential: 2^1=2, 2^2=4, 2^3=8 (jitter=0)
        assert sleep_times == pytest.approx([2.0, 4.0, 8.0])

    def test_rate_limit_does_not_consume_max_retries(
        self, _dl, _fn, _gmt, _rm,
    ):
        """Rate-limit retries should be independent of max_retries (default 2)."""
        resolver = _make_resolver(max_retries=2)

        # Hit rate limit 4 times (more than max_retries=2), then succeed
        call_count = {"n": 0}

        def upload_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] <= 4:
                raise RateLimitError()
            return {"file_token": "token_ok"}

        self._setup_upload_side_effect(resolver, upload_side_effect)

        with patch("workers.lib.attachment_token_resolver.time.sleep"):
            with patch("workers.lib.attachment_token_resolver.random.uniform",
                       return_value=0.0):
                result = resolver.resolve_single("http://example.com/img.jpg")

        # Should succeed despite 4 rate-limit retries > max_retries=2
        assert result == "token_ok"
        assert call_count["n"] == 5

    def test_rate_limit_gives_up_after_max_rate_limit_retries(
        self, _dl, _fn, _gmt, _rm,
    ):
        """After MAX_RATE_LIMIT_RETRIES (5) rate limit failures, return None."""
        resolver = _make_resolver()

        def always_rate_limit(*args, **kwargs):
            raise RateLimitError()

        self._setup_upload_side_effect(resolver, always_rate_limit)

        sleep_times = []
        with patch("workers.lib.attachment_token_resolver.time.sleep",
                   side_effect=lambda s: sleep_times.append(s)):
            with patch("workers.lib.attachment_token_resolver.random.uniform",
                       return_value=0.0):
                result = resolver.resolve_single("http://example.com/img.jpg")

        assert result is None
        # MAX_RATE_LIMIT_RETRIES=5 sleeps (the 6th attempt breaks before sleeping)
        assert len(sleep_times) == MAX_RATE_LIMIT_RETRIES
        # Verify exponential: 2, 4, 8, 16, 30 (capped at 30)
        assert sleep_times == pytest.approx([2.0, 4.0, 8.0, 16.0, 30.0])

    def test_rate_limit_sleep_capped_at_30_seconds(
        self, _dl, _fn, _gmt, _rm,
    ):
        """Sleep time should be capped at 30 seconds."""
        resolver = _make_resolver()

        # Force 6 rate limit attempts so we reach 2^6=64 → should be capped at 30
        call_count = {"n": 0}

        def upload_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] <= 5:
                raise RateLimitError()
            # 6th attempt also rate limit → triggers break
            raise RateLimitError()

        self._setup_upload_side_effect(resolver, upload_side_effect)

        sleep_times = []
        with patch("workers.lib.attachment_token_resolver.time.sleep",
                   side_effect=lambda s: sleep_times.append(s)):
            with patch("workers.lib.attachment_token_resolver.random.uniform",
                       return_value=0.0):
                resolver.resolve_single("http://example.com/img.jpg")

        # All sleeps capped at 30
        assert all(t <= 30.0 for t in sleep_times)

    def test_rate_limit_jitter_adds_random_component(
        self, _dl, _fn, _gmt, _rm,
    ):
        """Jitter should add random.uniform(0, base*0.5) to sleep time."""
        resolver = _make_resolver()

        call_count = {"n": 0}

        def upload_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] <= 1:
                raise RateLimitError()
            return {"file_token": "tok"}

        self._setup_upload_side_effect(resolver, upload_side_effect)

        jitter_value = 0.75
        sleep_times = []
        with patch("workers.lib.attachment_token_resolver.time.sleep",
                   side_effect=lambda s: sleep_times.append(s)):
            with patch("workers.lib.attachment_token_resolver.random.uniform",
                       return_value=jitter_value):
                result = resolver.resolve_single("http://example.com/img.jpg")

        assert result == "tok"
        # base=2^1=2, jitter=0.75 → sleep = 2.75
        assert sleep_times == pytest.approx([2.0 + jitter_value])

    # --- Normal retry tests -------------------------------------------------

    def test_normal_error_uses_original_backoff(
        self, _dl, _fn, _gmt, _rm,
    ):
        """Non-rate-limit retryable errors should use backoff_seconds * 2^(n-1)."""
        resolver = _make_resolver(max_retries=3, backoff_seconds=0.4)

        call_count = {"n": 0}

        def upload_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] <= 2:
                raise NormalRetryableError("connection reset")
            return {"file_token": "tok"}

        self._setup_upload_side_effect(resolver, upload_side_effect)

        sleep_times = []
        with patch("workers.lib.attachment_token_resolver.time.sleep",
                   side_effect=lambda s: sleep_times.append(s)):
            result = resolver.resolve_single("http://example.com/img.jpg")

        assert result == "tok"
        # 2 failures: 0.4 * 2^0 = 0.4, 0.4 * 2^1 = 0.8
        assert sleep_times == pytest.approx([0.4, 0.8])

    def test_normal_error_exhausts_max_retries(
        self, _dl, _fn, _gmt, _rm,
    ):
        """Non-retryable error or max_retries exceeded → returns None."""
        resolver = _make_resolver(max_retries=2, backoff_seconds=0.1)

        def always_fail(*args, **kwargs):
            raise NormalRetryableError("always fails")

        self._setup_upload_side_effect(resolver, always_fail)

        sleep_times = []
        with patch("workers.lib.attachment_token_resolver.time.sleep",
                   side_effect=lambda s: sleep_times.append(s)):
            result = resolver.resolve_single("http://example.com/img.jpg")

        assert result is None
        # max_retries=2 → 2 sleeps: 0.1*1, 0.1*2
        assert len(sleep_times) == 2

    def test_permanent_error_breaks_immediately(
        self, _dl, _fn, _gmt, _rm,
    ):
        """Permanent (non-retryable) error should break without sleeping."""
        resolver = _make_resolver(max_retries=3)

        def permanent_fail(*args, **kwargs):
            raise PermanentError()

        self._setup_upload_side_effect(resolver, permanent_fail)

        sleep_times = []
        with patch("workers.lib.attachment_token_resolver.time.sleep",
                   side_effect=lambda s: sleep_times.append(s)):
            result = resolver.resolve_single("http://example.com/img.jpg")

        assert result is None
        assert sleep_times == []  # No sleep, immediate break

    # --- Mixed scenario -----------------------------------------------------

    def test_rate_limit_then_normal_error_independent_counters(
        self, _dl, _fn, _gmt, _rm,
    ):
        """Rate limit and normal retries use independent counters."""
        resolver = _make_resolver(max_retries=1, backoff_seconds=0.1)

        call_count = {"n": 0}

        def mixed_side_effect(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RateLimitError()  # rate limit #1
            if call_count["n"] == 2:
                raise NormalRetryableError("conn reset")  # normal #1
            if call_count["n"] == 3:
                raise RateLimitError()  # rate limit #2 (independent)
            return {"file_token": "tok"}

        self._setup_upload_side_effect(resolver, mixed_side_effect)

        sleep_times = []
        with patch("workers.lib.attachment_token_resolver.time.sleep",
                   side_effect=lambda s: sleep_times.append(s)):
            with patch("workers.lib.attachment_token_resolver.random.uniform",
                       return_value=0.0):
                result = resolver.resolve_single("http://example.com/img.jpg")

        assert result == "tok"
        assert len(sleep_times) == 3
        # RL#1: 2^1=2, Normal#1: 0.1*2^0=0.1, RL#2: 2^2=4
        assert sleep_times == pytest.approx([2.0, 0.1, 4.0])
