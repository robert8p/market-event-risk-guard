"""Shared test fixtures — mocks network calls so tests are fast and deterministic."""

import pytest
from unittest.mock import AsyncMock, patch


@pytest.fixture(autouse=True)
def mock_http_calls():
    """Prevent all adapters and services from making real HTTP requests.
    Adapters return empty results instantly, making tests fast and hermetic."""
    with patch("app.adapters.base.BaseAdapter._fetch_json", new_callable=AsyncMock, return_value=None), \
         patch("app.adapters.base.BaseAdapter._fetch_text", new_callable=AsyncMock, return_value=None):
        yield
