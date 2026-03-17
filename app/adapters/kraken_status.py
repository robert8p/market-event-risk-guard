"""Kraken exchange status adapter (public Atlassian Statuspage, no key needed)."""

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


class KrakenStatusAdapter(BaseAdapter):
    """Fetches incidents and scheduled maintenance from status.kraken.com."""

    name = "KrakenStatusAdapter"
    INCIDENTS_URL = "https://status.kraken.com/api/v2/incidents.json"
    MAINTENANCE_URL = "https://status.kraken.com/api/v2/scheduled-maintenances.json"

    async def fetch_events(self) -> list[NormalisedEvent]:
        events: list[NormalisedEvent] = []

        # Active incidents
        data = await self._fetch_json(self.INCIDENTS_URL)
        if data and isinstance(data, dict):
            for inc in data.get("incidents", [])[:20]:
                try:
                    ev = self._normalise(inc, is_maintenance=False)
                    if ev:
                        events.append(ev)
                except Exception as exc:
                    logger.debug(f"[{self.name}] skip: {exc}")

        # Scheduled maintenance
        mdata = await self._fetch_json(self.MAINTENANCE_URL)
        if mdata and isinstance(mdata, dict):
            for m in mdata.get("scheduled_maintenances", [])[:10]:
                try:
                    ev = self._normalise(m, is_maintenance=True)
                    if ev:
                        events.append(ev)
                except Exception:
                    pass

        self._health.event_count = len(events)
        return events

    def _normalise(self, inc: dict, is_maintenance: bool) -> NormalisedEvent | None:
        title = inc.get("name", "")
        if not title:
            return None

        status = inc.get("status", "")
        if status in ("resolved", "completed", "postmortem"):
            return None

        created = inc.get("created_at") or inc.get("scheduled_for") or ""
        if not created:
            return None

        try:
            start_utc = datetime.fromisoformat(created.replace("Z", "+00:00"))
            if start_utc.tzinfo is None:
                start_utc = start_utc.replace(tzinfo=timezone.utc)
        except Exception:
            return None

        # Parse end time for maintenance
        end_utc = None
        scheduled_until = inc.get("scheduled_until")
        if scheduled_until:
            try:
                end_utc = datetime.fromisoformat(scheduled_until.replace("Z", "+00:00"))
                if end_utc.tzinfo is None:
                    end_utc = end_utc.replace(tzinfo=timezone.utc)
            except Exception:
                pass

        classification = Classification.SCHEDULED if is_maintenance else Classification.ONGOING
        event_type = "exchange_maintenance" if is_maintenance else "exchange_outage"

        eid = hashlib.md5(f"krk-{inc.get('id', title)}".encode()).hexdigest()[:12]

        impact = inc.get("impact", "none")
        description = f"Kraken {impact} — {status}"

        return NormalisedEvent(
            id=f"krk-{eid}",
            title=f"Kraken: {title}",
            category=EventCategory.CRYPTO,
            event_type=event_type,
            classification=classification,
            asset_class=AssetClass.CRYPTO,
            start_time_utc=start_utc,
            end_time_utc=end_utc,
            impact_score=0,
            caution_level="Low",
            suggested_action="Technicals usable",
            confidence=0.8,
            source_name=self.name,
            source_url=inc.get("shortlink", "https://status.kraken.com"),
            affected_instruments=["BTC-USD", "ETH-USD", "SOL-USD"],
            description=description,
        )
