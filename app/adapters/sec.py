"""SEC newsroom RSS adapter for regulatory actions and press releases."""

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

SEC_KEYWORDS = {
    # High-impact — actual market-moving actions
    "etf": "etf_decision",
    "crypto": "crypto_regulatory",
    "bitcoin": "crypto_regulatory",
    "digital asset": "crypto_regulatory",
    "fraud": "sec_action",
    "settlement": "sec_action",
    "litigation": "sec_action",
    "complaint": "sec_action",
    "enforcement action": "sec_action",
    "halted": "sec_action",
    "suspended": "sec_action",
    # Moderate — rule changes that take time to affect markets
    "rule": "sec_decision",
    "proposal": "sec_routine",
    "proposes": "sec_routine",
    "approval": "sec_decision",
    # Low-impact — routine administrative
    "resign": "sec_routine",
    "appoint": "sec_routine",
    "announce": "sec_routine",
    "report": "sec_routine",
    "statement": "sec_routine",
    "director": "sec_routine",
    "commissioner": "sec_routine",
    "staff": "sec_routine",
}


class SecNewsAdapter(BaseAdapter):
    """Fetches SEC press releases from the official RSS feed."""

    name = "SecNewsAdapter"
    RSS_URL = "https://www.sec.gov/news/pressreleases.rss"

    async def fetch_events(self) -> list[NormalisedEvent]:
        text = await self._fetch_text(
            self.RSS_URL,
            headers={"User-Agent": "MarketEventRiskGuard/1.0 (contact@example.com)"},
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

        start_utc = self._parse_date(pub_date)
        if not start_utc or start_utc < lookback:
            return None

        event_type, asset_class = self._classify(title + " " + desc)
        eid = hashlib.md5(f"sec-{title}-{pub_date}".encode()).hexdigest()[:12]

        return NormalisedEvent(
            id=f"sec-{eid}",
            title=f"SEC: {title}",
            category=EventCategory.EQUITY,
            event_type=event_type,
            classification=Classification.BREAKING,
            asset_class=asset_class,
            start_time_utc=start_utc,
            impact_score=0,
            caution_level="Low",
            suggested_action="Technicals usable",
            confidence=0.85,
            source_name=self.name,
            source_url=link or "https://www.sec.gov/newsroom",
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
    def _classify(text: str) -> tuple[str, AssetClass]:
        t = text.lower()
        for kw, etype in SEC_KEYWORDS.items():
            if kw in t:
                ac = AssetClass.CRYPTO if "crypto" in etype else AssetClass.EQUITIES
                return etype, ac
        return "sec_action", AssetClass.EQUITIES
