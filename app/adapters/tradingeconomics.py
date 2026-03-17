"""TradingEconomics economic calendar adapter."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta, timezone

from app.adapters.base import BaseAdapter
from app.models import (
    AssetClass,
    Classification,
    EventCategory,
    NormalisedEvent,
)

logger = logging.getLogger(__name__)

# Map TE importance to rough event-type hint
IMPORTANCE_MAP = {
    1: "minor_economic",
    2: "economic",
    3: "major_economic",
}

# Known high-impact event keywords
HIGH_IMPACT_KEYWORDS = {
    "interest rate": "rate_decision",
    "fed funds": "fed_rate",
    "fomc": "fomc",
    "cpi": "cpi",
    "consumer price": "cpi",
    "nonfarm": "nonfarm_payrolls",
    "non-farm": "nonfarm_payrolls",
    "ppi": "ppi",
    "producer price": "ppi",
    "gdp": "gdp",
    "pmi": "pmi",
    "ism": "ism",
    "retail sales": "retail_sales",
    "unemployment": "unemployment",
    "consumer confidence": "consumer_confidence",
    "minutes": "central_bank_minutes",
    "speech": "central_bank_speech",
    "treasury": "treasury_auction",
    "tariff": "tariffs",
    "sanction": "sanctions",
}


class TradingEconomicsAdapter(BaseAdapter):
    name = "TradingEconomicsAdapter"

    async def fetch_events(self) -> list[NormalisedEvent]:
        s = self.settings
        if not self._is_valid_key(s.tradingeconomics_client_key):
            logger.info(f"[{self.name}] No API key configured, skipping.")
            self._health.status = "needs_key"
            self._health.last_error = "No API key"
            return []

        now = datetime.now(timezone.utc)
        start = now.strftime("%Y-%m-%d")
        end = (now + timedelta(hours=s.default_window_hours + 24)).strftime("%Y-%m-%d")

        url = f"{s.tradingeconomics_base_url}/calendar"
        params = {
            "c": f"{s.tradingeconomics_client_key}:{s.tradingeconomics_client_secret}",
            "d1": start,
            "d2": end,
            "f": "json",
        }

        data = await self._fetch_json(url, params=params)
        if not data or not isinstance(data, list):
            return []

        events: list[NormalisedEvent] = []
        for item in data:
            try:
                ev = self._normalise(item)
                if ev:
                    events.append(ev)
            except Exception as exc:
                logger.debug(f"[{self.name}] skip item: {exc}")

        self._health.event_count = len(events)
        return events

    def _normalise(self, item: dict) -> NormalisedEvent | None:
        title = item.get("Event") or item.get("Category") or ""
        if not title:
            return None

        date_str = item.get("Date") or item.get("DateUtc") or ""
        if not date_str:
            return None

        try:
            start_utc = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if start_utc.tzinfo is None:
                start_utc = start_utc.replace(tzinfo=timezone.utc)
        except Exception:
            return None

        importance = item.get("Importance", 1)
        country = item.get("Country", "")
        event_type = self._classify_event_type(title)

        eid = hashlib.md5(f"te-{title}-{date_str}".encode()).hexdigest()[:12]

        return NormalisedEvent(
            id=f"te-{eid}",
            title=f"{country}: {title}" if country else title,
            category=EventCategory.MACRO,
            event_type=event_type,
            classification=Classification.SCHEDULED,
            asset_class=AssetClass.BOTH if importance >= 3 else AssetClass.EQUITIES,
            start_time_utc=start_utc,
            impact_score=0,
            caution_level="Low",
            suggested_action="Technicals usable",
            confidence=0.5,
            source_name=self.name,
            source_url="https://tradingeconomics.com/calendar",
            description=f"{country} {title} — Importance {importance}/3",
        )

    @staticmethod
    def _classify_event_type(title: str) -> str:
        t = title.lower()
        for keyword, etype in HIGH_IMPACT_KEYWORDS.items():
            if keyword in t:
                return etype
        return "economic"
