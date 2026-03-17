"""Coinbase and Coinbase Exchange status adapters (public, no key needed)."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone

from app.adapters.base import BaseAdapter
from app.models import (
    AssetClass,
    Classification,
    EventCategory,
    NormalisedEvent,
)

logger = logging.getLogger(__name__)

STATUSPAGE_STATUSES = {"investigating", "identified", "monitoring", "in_progress", "scheduled"}


class CoinbaseStatusAdapter(BaseAdapter):
    """Fetches incidents from https://status.coinbase.com (Atlassian Statuspage)."""

    name = "CoinbaseStatusAdapter"
    STATUS_URL = "https://status.coinbase.com/api/v2/incidents.json"

    async def fetch_events(self) -> list[NormalisedEvent]:
        data = await self._fetch_json(self.STATUS_URL)
        if not data or not isinstance(data, dict):
            return []

        incidents = data.get("incidents", [])
        events: list[NormalisedEvent] = []
        for inc in incidents[:20]:
            try:
                ev = self._normalise(inc)
                if ev:
                    events.append(ev)
            except Exception as exc:
                logger.debug(f"[{self.name}] skip: {exc}")

        # Also check scheduled maintenances
        maint_data = await self._fetch_json("https://status.coinbase.com/api/v2/scheduled-maintenances.json")
        if maint_data and isinstance(maint_data, dict):
            for m in maint_data.get("scheduled_maintenances", [])[:10]:
                try:
                    ev = self._normalise(m, is_maintenance=True)
                    if ev:
                        events.append(ev)
                except Exception:
                    pass

        self._health.event_count = len(events)
        return events

    def _normalise(self, inc: dict, is_maintenance: bool = False) -> NormalisedEvent | None:
        title = inc.get("name", "")
        if not title:
            return None

        status = inc.get("status", "")
        created = inc.get("created_at") or inc.get("scheduled_for") or ""
        if not created:
            return None

        try:
            start_utc = datetime.fromisoformat(created.replace("Z", "+00:00"))
            if start_utc.tzinfo is None:
                start_utc = start_utc.replace(tzinfo=timezone.utc)
        except Exception:
            return None

        if status == "resolved" or status == "completed":
            return None

        classification = Classification.SCHEDULED if is_maintenance else Classification.ONGOING
        event_type = "exchange_maintenance" if is_maintenance else "exchange_outage"

        eid = hashlib.md5(f"cb-{inc.get('id', title)}".encode()).hexdigest()[:12]

        return NormalisedEvent(
            id=f"cb-{eid}",
            title=f"Coinbase: {title}",
            category=EventCategory.CRYPTO,
            event_type=event_type,
            classification=classification,
            asset_class=AssetClass.CRYPTO,
            start_time_utc=start_utc,
            impact_score=0,
            caution_level="Low",
            suggested_action="Technicals usable",
            confidence=0.8,
            source_name=self.name,
            source_url=inc.get("shortlink", "https://status.coinbase.com"),
            description=inc.get("impact", "") + " — " + status,
        )


class CoinbaseExchangeStatusAdapter(BaseAdapter):
    """Fetches incidents from https://cdpstatus.coinbase.com (CDP / Exchange)."""

    name = "CoinbaseExchangeStatusAdapter"
    STATUS_URL = "https://cdpstatus.coinbase.com/api/v2/incidents.json"

    async def fetch_events(self) -> list[NormalisedEvent]:
        data = await self._fetch_json(self.STATUS_URL)
        if not data or not isinstance(data, dict):
            return []

        incidents = data.get("incidents", [])
        events: list[NormalisedEvent] = []
        for inc in incidents[:20]:
            try:
                ev = self._normalise(inc)
                if ev:
                    events.append(ev)
            except Exception as exc:
                logger.debug(f"[{self.name}] skip: {exc}")

        self._health.event_count = len(events)
        return events

    def _normalise(self, inc: dict) -> NormalisedEvent | None:
        title = inc.get("name", "")
        if not title:
            return None

        status = inc.get("status", "")
        created = inc.get("created_at") or ""
        if not created:
            return None

        try:
            start_utc = datetime.fromisoformat(created.replace("Z", "+00:00"))
            if start_utc.tzinfo is None:
                start_utc = start_utc.replace(tzinfo=timezone.utc)
        except Exception:
            return None

        if status == "resolved" or status == "completed":
            return None

        eid = hashlib.md5(f"cbx-{inc.get('id', title)}".encode()).hexdigest()[:12]

        return NormalisedEvent(
            id=f"cbx-{eid}",
            title=f"Coinbase Exchange: {title}",
            category=EventCategory.CRYPTO,
            event_type="exchange_outage",
            classification=Classification.ONGOING,
            asset_class=AssetClass.CRYPTO,
            start_time_utc=start_utc,
            impact_score=0,
            caution_level="Low",
            suggested_action="Technicals usable",
            confidence=0.8,
            source_name=self.name,
            source_url=inc.get("shortlink", "https://cdpstatus.coinbase.com"),
            description=inc.get("impact", "") + " — " + status,
        )
