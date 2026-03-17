"""Federal Reserve calendar and press adapters (public RSS/HTML scraping)."""

from __future__ import annotations

import hashlib
import logging
import re
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

FED_EVENT_KEYWORDS = {
    "fomc": "fomc",
    "rate": "rate_decision",
    "interest": "rate_decision",
    "minutes": "central_bank_minutes",
    "speech": "central_bank_speech",
    "testimony": "central_bank_speech",
    "press conference": "fomc",
    "monetary policy": "fomc",
    "beige book": "central_bank_minutes",
    "statement": "fomc",
}


class FedCalendarAdapter(BaseAdapter):
    """Scrapes the Fed's public RSS feed for upcoming calendar events."""

    name = "FedCalendarAdapter"
    RSS_URL = "https://www.federalreserve.gov/feeds/press_all.xml"

    async def fetch_events(self) -> list[NormalisedEvent]:
        text = await self._fetch_text(self.RSS_URL)
        if not text:
            return []

        events: list[NormalisedEvent] = []
        try:
            root = ElementTree.fromstring(text)
            channel = root.find("channel")
            if channel is None:
                return []

            for item in channel.findall("item"):
                try:
                    ev = self._normalise_item(item)
                    if ev:
                        events.append(ev)
                except Exception as exc:
                    logger.debug(f"[{self.name}] skip item: {exc}")
        except ElementTree.ParseError as exc:
            logger.warning(f"[{self.name}] XML parse error: {exc}")
            self._health.status = "failed"
            self._health.last_error = str(exc)

        self._health.event_count = len(events)
        return events

    def _normalise_item(self, item) -> NormalisedEvent | None:
        title = (item.findtext("title") or "").strip()
        if not title:
            return None

        pub_date = (item.findtext("pubDate") or "").strip()
        link = (item.findtext("link") or "").strip()
        desc = (item.findtext("description") or "").strip()

        start_utc = self._parse_date(pub_date)
        if not start_utc:
            return None

        event_type = self._classify(title + " " + desc)
        eid = hashlib.md5(f"fed-{title}-{pub_date}".encode()).hexdigest()[:12]

        return NormalisedEvent(
            id=f"fed-{eid}",
            title=f"Fed: {title}",
            category=EventCategory.MACRO,
            event_type=event_type,
            classification=Classification.SCHEDULED,
            asset_class=AssetClass.BOTH,
            start_time_utc=start_utc,
            impact_score=0,
            caution_level="Low",
            suggested_action="Technicals usable",
            confidence=0.85,
            source_name=self.name,
            source_url=link or "https://www.federalreserve.gov/newsevents/calendar.htm",
            description=desc[:500] if desc else "",
        )

    @staticmethod
    def _parse_date(date_str: str) -> datetime | None:
        formats = [
            "%a, %d %b %Y %H:%M:%S %z",
            "%a, %d %b %Y %H:%M:%S %Z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d",
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
        for kw, etype in FED_EVENT_KEYWORDS.items():
            if kw in t:
                return etype
        return "central_bank_speech"


class FedPressAdapter(BaseAdapter):
    """Scrapes the Fed's press-release RSS feed for breaking developments."""

    name = "FedPressAdapter"
    RSS_URL = "https://www.federalreserve.gov/feeds/press_all.xml"

    async def fetch_events(self) -> list[NormalisedEvent]:
        text = await self._fetch_text(self.RSS_URL)
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
                    logger.debug(f"[{self.name}] skip item: {exc}")
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

        start_utc = FedCalendarAdapter._parse_date(pub_date)
        if not start_utc or start_utc < lookback:
            return None

        event_type = FedCalendarAdapter._classify(title + " " + desc)
        eid = hashlib.md5(f"fedp-{title}-{pub_date}".encode()).hexdigest()[:12]

        return NormalisedEvent(
            id=f"fedp-{eid}",
            title=f"Fed Press: {title}",
            category=EventCategory.MACRO,
            event_type=event_type,
            classification=Classification.BREAKING,
            asset_class=AssetClass.BOTH,
            start_time_utc=start_utc,
            impact_score=0,
            caution_level="Low",
            suggested_action="Technicals usable",
            confidence=0.9,
            source_name=self.name,
            source_url=link or "https://www.federalreserve.gov/newsevents/pressreleases.htm",
            description=desc[:500] if desc else "",
        )
