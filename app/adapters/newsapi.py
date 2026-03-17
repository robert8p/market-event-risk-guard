"""NewsAPI breaking-news adapter with keyword classification."""

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

# Keyword → (event_type, category, asset_class)
# ORDER MATTERS: first match wins. Specific phrases must appear before generic keywords.
KEYWORD_RULES: list[tuple[list[str], str, EventCategory, AssetClass]] = [
    # Macro / cross-asset
    (["fed ", "fomc", "federal reserve"], "fomc", EventCategory.MACRO, AssetClass.BOTH),
    (["cpi", "inflation data", "consumer price"], "cpi", EventCategory.MACRO, AssetClass.BOTH),
    (["payroll", "nonfarm", "jobs report"], "nonfarm_payrolls", EventCategory.MACRO, AssetClass.BOTH),
    (["tariff"], "tariffs", EventCategory.MACRO, AssetClass.BOTH),
    (["sanction"], "sanctions", EventCategory.MACRO, AssetClass.BOTH),
    (["geopolitical", "war ", "conflict", "missile", "invasion"], "geopolitical", EventCategory.CROSS_ASSET, AssetClass.BOTH),
    # Crypto — SPECIFIC phrases first, then generic tokens
    (["crypto etf", "bitcoin etf", "spot etf", "ether etf"], "etf_decision", EventCategory.CRYPTO, AssetClass.CRYPTO),
    (["stablecoin", "usdt", "usdc", "depeg"], "stablecoin_depeg", EventCategory.CRYPTO, AssetClass.CRYPTO),
    (["exchange outage", "binance down", "coinbase down"], "exchange_outage", EventCategory.CRYPTO, AssetClass.CRYPTO),
    (["delisting"], "delisting", EventCategory.CRYPTO, AssetClass.CRYPTO),
    (["listing"], "listing", EventCategory.CRYPTO, AssetClass.CRYPTO),
    (["bitcoin", "btc"], "crypto_regulatory", EventCategory.CRYPTO, AssetClass.CRYPTO),
    (["ethereum", "eth"], "crypto_regulatory", EventCategory.CRYPTO, AssetClass.CRYPTO),
    # Equity / regulatory — specific ETF after crypto ETF
    (["sec ", "securities and exchange"], "sec_action", EventCategory.EQUITY, AssetClass.EQUITIES),
    (["etf"], "etf_decision", EventCategory.EQUITY, AssetClass.EQUITIES),
]


class NewsApiBreakingAdapter(BaseAdapter):
    name = "NewsApiBreakingAdapter"

    async def fetch_events(self) -> list[NormalisedEvent]:
        s = self.settings
        if not self._is_valid_key(s.newsapi_key):
            logger.info(f"[{self.name}] No API key configured, skipping.")
            self._health.status = "needs_key"
            self._health.last_error = "No API key"
            return []

        now = datetime.now(timezone.utc)
        from_dt = (now - timedelta(hours=s.news_lookback_hours)).strftime("%Y-%m-%dT%H:%M:%S")

        url = f"{s.newsapi_base_url}/everything"
        params = {
            "q": s.breaking_news_query,
            "from": from_dt,
            "sortBy": "publishedAt",
            "language": "en",
            "pageSize": 50,
            "apiKey": s.newsapi_key,
        }

        data = await self._fetch_json(url, params=params)
        if not data or not isinstance(data, dict):
            return []

        articles = data.get("articles", [])
        events: list[NormalisedEvent] = []

        for article in articles:
            try:
                ev = self._normalise(article)
                if ev:
                    events.append(ev)
            except Exception as exc:
                logger.debug(f"[{self.name}] skip article: {exc}")

        self._health.event_count = len(events)
        return events

    def _normalise(self, article: dict) -> NormalisedEvent | None:
        title = article.get("title") or ""
        if not title or title == "[Removed]":
            return None

        pub = article.get("publishedAt") or ""
        if not pub:
            return None

        try:
            start_utc = datetime.fromisoformat(pub.replace("Z", "+00:00"))
            if start_utc.tzinfo is None:
                start_utc = start_utc.replace(tzinfo=timezone.utc)
        except Exception:
            return None

        description = article.get("description") or ""
        combined = (title + " " + description).lower()

        event_type, category, asset_class = self._classify(combined)

        eid = hashlib.md5(f"news-{title}-{pub}".encode()).hexdigest()[:12]

        return NormalisedEvent(
            id=f"news-{eid}",
            title=title,
            category=category,
            event_type=event_type,
            classification=Classification.BREAKING,
            asset_class=asset_class,
            start_time_utc=start_utc,
            impact_score=0,
            caution_level="Low",
            suggested_action="Technicals usable",
            confidence=0.4,
            source_name=article.get("source", {}).get("name", self.name),
            source_url=article.get("url", ""),
            description=description[:500] if description else "",
        )

    @staticmethod
    def _classify(text: str) -> tuple[str, EventCategory, AssetClass]:
        for keywords, etype, cat, ac in KEYWORD_RULES:
            for kw in keywords:
                if kw in text:
                    return etype, cat, ac
        return "breaking_news", EventCategory.CROSS_ASSET, AssetClass.BOTH
