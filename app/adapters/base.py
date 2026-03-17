"""Base class for all source adapters."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional

import httpx

from app.config import get_settings
from app.models import NormalisedEvent, SourceHealth

logger = logging.getLogger(__name__)


class BaseAdapter(ABC):
    """Every source adapter extends this base."""

    name: str = "BaseAdapter"
    _PLACEHOLDER_VALUES = frozenset({
        "", "your_key_here", "your_secret_here", "your_api_key",
        "your-key-here", "PASTE_KEY_HERE", "xxx", "test", "none",
        "changeme", "replace_me", "TODO",
    })

    def __init__(self) -> None:
        self.settings = get_settings()
        self._health = SourceHealth(name=self.name, enabled=True, healthy=True)
        self._client: Optional[httpx.AsyncClient] = None

    @classmethod
    def _is_valid_key(cls, key: str | None) -> bool:
        """Return False if key is missing, empty, or a known placeholder."""
        if not key:
            return False
        return key.strip().lower() not in cls._PLACEHOLDER_VALUES

    # ── HTTP helper ──────────────────────────────────────────────────────

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.settings.http_timeout_seconds),
                follow_redirects=True,
            )
        return self._client

    async def _fetch_json(self, url: str, params: dict | None = None, headers: dict | None = None) -> dict | list | None:
        try:
            client = await self._get_client()
            resp = await client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            self._health.status = "healthy"
            self._health.last_fetch_utc = datetime.now(timezone.utc)
            return resp.json()
        except Exception as exc:
            logger.warning(f"[{self.name}] fetch failed: {exc}")
            self._health.status = "failed"
            self._health.last_error = str(exc)
            self._health.last_fetch_utc = datetime.now(timezone.utc)
            return None

    async def _fetch_text(self, url: str, params: dict | None = None, headers: dict | None = None) -> str | None:
        try:
            client = await self._get_client()
            resp = await client.get(url, params=params, headers=headers)
            resp.raise_for_status()
            self._health.status = "healthy"
            self._health.last_fetch_utc = datetime.now(timezone.utc)
            return resp.text
        except Exception as exc:
            logger.warning(f"[{self.name}] fetch failed: {exc}")
            self._health.status = "failed"
            self._health.last_error = str(exc)
            self._health.last_fetch_utc = datetime.now(timezone.utc)
            return None

    # ── Abstract interface ───────────────────────────────────────────────

    @abstractmethod
    async def fetch_events(self) -> list[NormalisedEvent]:
        """Return normalised events from this source."""
        ...

    def health(self) -> SourceHealth:
        return self._health

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
