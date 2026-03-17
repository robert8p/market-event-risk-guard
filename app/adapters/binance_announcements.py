"""Binance announcements adapter (public endpoint, no key needed).

Covers listings, delistings, maintenance, wallet issues, and new trading pairs.
"""

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

# Binance catalogue IDs for announcement types
CATALOGUE_IDS = {
    48: "new_listings",
    49: "delistings",
    161: "wallet_maintenance",
    128: "airdrop",
}

BINANCE_KEYWORDS = {
    # Order matters — first match wins. Put specific before general.
    "delist": "delisting",
    "removal of": "delisting",
    "will list ": "listing",
    "will be listed": "listing",
    "new listing": "listing",
    "hard fork": "hard_fork",
    "suspend": "exchange_outage",
    "airdrop": "token_unlock",
    # Routine / low-impact — these should NOT trigger high scores
    "tick size": "binance_routine",
    "trading bots": "binance_routine",
    "trading pair": "binance_routine",
    "margin & futures": "binance_routine",
    "buy crypto": "binance_routine",
    "convert": "binance_routine",
    "earn": "binance_routine",
    "vip": "binance_routine",
    "loan": "binance_routine",
    "copy trading": "binance_routine",
    "update on": "binance_routine",
    "updates on": "binance_routine",
    "notice on": "binance_routine",
    "perpetual": "binance_routine",
    "leverage": "binance_routine",
    "seed tag": "binance_routine",
    # General — only after specifics
    "maintenance": "exchange_maintenance",
    "wallet": "exchange_maintenance",
    "upgrade": "protocol_upgrade",
    "network": "protocol_upgrade",
}


class BinanceAnnouncementsAdapter(BaseAdapter):
    """Fetches recent Binance announcements from the public CMS API."""

    name = "BinanceAnnouncementsAdapter"
    API_URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"

    async def fetch_events(self) -> list[NormalisedEvent]:
        events: list[NormalisedEvent] = []

        # Fetch new listings and delistings
        for cat_id, cat_label in CATALOGUE_IDS.items():
            try:
                batch = await self._fetch_category(cat_id, cat_label)
                events.extend(batch)
            except Exception as exc:
                logger.debug(f"[{self.name}] category {cat_id} failed: {exc}")

        self._health.event_count = len(events)
        return events

    async def _fetch_category(self, catalogue_id: int, label: str) -> list[NormalisedEvent]:
        params = {
            "type": 1,
            "catalogId": catalogue_id,
            "pageNo": 1,
            "pageSize": 20,
        }

        data = await self._fetch_json(self.API_URL, params=params)
        if not data or not isinstance(data, dict):
            return []

        articles = data.get("data", {}).get("catalogs", [{}])
        if not articles:
            return []

        # The articles are nested under catalogs[0].articles
        catalog_data = articles[0] if articles else {}
        article_list = catalog_data.get("articles", [])

        now = datetime.now(timezone.utc)
        lookback = now - timedelta(hours=self.settings.news_lookback_hours * 2)

        results: list[NormalisedEvent] = []
        for article in article_list:
            try:
                ev = self._normalise(article, label)
                if ev and ev.start_time_utc >= lookback:
                    results.append(ev)
            except Exception as exc:
                logger.debug(f"[{self.name}] skip article: {exc}")

        return results

    def _normalise(self, article: dict, default_type: str) -> NormalisedEvent | None:
        title = article.get("title", "")
        if not title:
            return None

        release_date = article.get("releaseDate")
        if not release_date:
            return None

        try:
            # Binance timestamps are milliseconds
            if isinstance(release_date, (int, float)):
                start_utc = datetime.fromtimestamp(release_date / 1000, tz=timezone.utc)
            else:
                start_utc = datetime.fromisoformat(str(release_date).replace("Z", "+00:00"))
                if start_utc.tzinfo is None:
                    start_utc = start_utc.replace(tzinfo=timezone.utc)
        except Exception:
            return None

        event_type = self._classify(title, default_type)
        code = article.get("code", "")

        eid = hashlib.md5(f"bnc-{title}-{release_date}".encode()).hexdigest()[:12]

        return NormalisedEvent(
            id=f"bnc-{eid}",
            title=f"Binance: {title}",
            category=EventCategory.CRYPTO,
            event_type=event_type,
            classification=Classification.BREAKING,
            asset_class=AssetClass.CRYPTO,
            start_time_utc=start_utc,
            impact_score=0,
            caution_level="Low",
            suggested_action="Technicals usable",
            confidence=0.75,
            source_name=self.name,
            source_url=f"https://www.binance.com/en/support/announcement/{code}" if code else "https://www.binance.com/en/support/announcement",
            affected_instruments=["BTC-USD", "ETH-USD"],
            description=title,
        )

    @staticmethod
    def _classify(title: str, default_type: str) -> str:
        t = title.lower()
        for keyword, etype in BINANCE_KEYWORDS.items():
            if keyword in t:
                return etype
        return "binance_routine"
