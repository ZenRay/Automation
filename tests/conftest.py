"""Shared pytest fixtures and markers for Automation tests."""

import pytest


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: marks tests that require external API access "
        '(deselect with \'-m "not integration"\')',
    )


@pytest.fixture
def client():
    """Fixture for Lark API client — only available in integration tests.

    Tests using this fixture should be marked with @pytest.mark.integration.
    """
    try:
        from automation.client import LarkMultiDimTable

        return LarkMultiDimTable()
    except Exception:
        pytest.skip("Lark API client not available (integration test)")
