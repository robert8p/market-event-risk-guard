"""European Central Bank calendar/press RSS adapter (free, no key needed)."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta, timezone
from xml.etree import ElementTree

from app.adapters.base import BaseAdapter
from app.models import (
    AssetClass,
    Classification,
    EventCategory,
    NormalisedEvent,
)

logger = logging.getLogger(__name__)

ECB_KEYWORDS = {
    "interest rate": "ecb_rate",
    "monetary policy": "ecb_rate",
    "governing council": "ecb_rate",
    "rate decision": "ecb_rate",
    "press conference": "ecb_rate",
    "inflation": "cpi",
    "speech": "central_bank_speech",
    "lagarde": "central_bank_speech",
}


class EcbCalendarAdapter(BaseAdapter):
    """Fetches ECB press releases from the official RSS feed."""

    name = "EcbCalendarAdapter"
    RSS_URL = "https://www.ecb.europa.eu/rss/press.html"

    async def fetch_events(self) -> list[NormalisedEvent]:
        text = await self._fetch_text(
            self.RSS_URL,
            headers={"User-Agent": "MarketEventRiskGuard/1.0"},
        )
        if not text:
            return []

        events: list[NormalisedEvent] = []
        try:
            root = ElementTree.fromstring(text)
            channel = root.find("channel")
            if channel is None:
                return []

            now = datetime.now(timezone.utc)
            lookback = now - timedelta(hours=self.settings.news_lookback_hours)

            for item in channel.findall("item"):
                try:
                    ev = self._normalise_item(item, lookback)
                    if ev:
                        events.append(ev)
                except Exception as exc:
                    logger.debug(f"[{self.name}] skip: {exc}")
        except ElementTree.ParseError as exc:
            logger.warning(f"[{self.name}] XML parse error: {exc}")
            self._health.status = "failed"
            self._health.last_error = str(exc)

        self._health.event_count = len(events)
        return events

    def _normalise_item(self, item, lookback: datetime) -> NormalisedEvent | None:
        title = (item.findtext("title") or "").strip()
        if not title:
            return None

        pub_date = (item.findtext("pubDate") or "").strip()
        link = (item.findtext("link") or "").strip()
        desc = (item.findtext("description") or "").strip()

        start_utc = self._parse_date(pub_date)
        if not start_utc or start_utc < lookback:
            return None

        event_type = self._classify(title + " " + desc)
        eid = hashlib.md5(f"ecb-{title}-{pub_date}".encode()).hexdigest()[:12]

        return NormalisedEvent(
            id=f"ecb-{eid}",
            title=f"ECB: {title}",
            category=EventCategory.MACRO,
            event_type=event_type,
            classification=Classification.BREAKING,
            asset_class=AssetClass.BOTH,
            start_time_utc=start_utc,
            impact_score=0,
            caution_level="Low",
            suggested_action="Technicals usable",
            confidence=0.85,
            source_name=self.name,
            source_url=link or "https://www.ecb.europa.eu/press/pr/html/index.en.html",
            affected_instruments=["ES", "NQ", "DXY", "EUR/USD", "rates-sensitive assets"],
            description=desc[:500] if desc else "",
        )

    @staticmethod
    def _parse_date(date_str: str) -> datetime | None:
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%S%z",
        ]
        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        return None

    @staticmethod
    def _classify(text: str) -> str:
        t = text.lower()
        for kw, etype in ECB_KEYWORDS.items():
            if kw in t:
                return etype
        return "central_bank_speech"
