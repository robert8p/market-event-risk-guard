"""
GDELT geopolitical and conflict news adapter (free, no key needed).

Uses the GDELT DOC 2.0 API to search for market-moving geopolitical
developments across global news in 100+ languages, updated every 15 minutes.

API docs: https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime, timedelta, timezone

import httpx

from app.adapters.base import BaseAdapter
from app.models import (
    AssetClass,
    Classification,
    EventCategory,
    NormalisedEvent,
)

logger = logging.getLogger(__name__)


class GdeltRateLimitError(Exception):
    def __init__(self, retry_after_seconds: int | None = None):
        self.retry_after_seconds = retry_after_seconds
        msg = "GDELT rate-limited"
        if retry_after_seconds:
            msg += f"; retry after {retry_after_seconds}s"
        super().__init__(msg)


GEOPOLITICAL_QUERIES = [
    (
        "(airstrike OR missile OR invasion OR bombardment OR shelling) (killed OR casualties OR destroyed OR retaliation)",
        "war",
        "Armed conflict or military strike with reported impact.",
        ["ES", "NQ", "CL", "GC", "BTC-USD", "DXY", "VIX"],
    ),
    (
        '(sanctions OR embargo OR "trade war" OR "export controls") (imposed OR announced OR escalation OR retaliation)',
        "sanctions",
        "Sanctions or trade restriction escalation detected.",
        ["ES", "NQ", "SPY", "DXY", "CL", "BTC-USD"],
    ),
    (
        "(Iran OR Israel OR Taiwan OR Ukraine OR Russia) (attack OR strike OR escalation OR offensive OR mobilization)",
        "conflict",
        "Geopolitical flashpoint escalation involving a major power.",
        ["ES", "NQ", "CL", "GC", "DXY", "BTC-USD"],
    ),
    (
        '("emergency rate cut" OR "emergency Fed meeting" OR "bank failure" OR "systemic risk" OR "stock market crash" OR "financial crisis")',
        "emergency_cb_action",
        "Emergency policy action or systemic financial risk detected.",
        ["ES", "NQ", "SPY", "TLT", "BTC-USD", "ETH-USD", "DXY"],
    ),
]


class GdeltGeopoliticalAdapter(BaseAdapter):
    name = "GdeltGeopoliticalAdapter"
    API_BASE = "https://api.gdeltproject.org/api/v2/doc/doc"

    def __init__(self) -> None:
        super().__init__()
        self._events_cache: list[NormalisedEvent] = []
        self._events_cache_time: datetime | None = None
        self._cooldown_until: datetime | None = None

    async def fetch_events(self) -> list[NormalisedEvent]:
        now = datetime.now(timezone.utc)

        # GDELT is too rate-limited for 60-second polling. Reuse a recent
        # snapshot for 15 minutes to keep the event engine stable.
        if self._events_cache_time and (now - self._events_cache_time).total_seconds() < 900:
            return list(self._events_cache)

        # If the upstream asked us to slow down, serve stale cache instead of
        # hammering it and flipping the app between results and failures.
        if self._cooldown_until and now < self._cooldown_until:
            self._health.status = "failed"
            self._health.last_error = f"GDELT cooling down until {self._cooldown_until.isoformat()}"
            return list(self._events_cache)

        events: list[NormalisedEvent] = []
        rate_limited = False

        for i, (query_str, event_type, description, instruments) in enumerate(GEOPOLITICAL_QUERIES):
            if i > 0:
                await asyncio.sleep(2)  # Rate-limit: 2s between queries
            try:
                batch = await self._search_query(query_str, event_type, description, instruments)
                events.extend(batch)
            except GdeltRateLimitError as exc:
                rate_limited = True
                self._enter_cooldown(exc.retry_after_seconds)
                logger.warning(f"[{self.name}] query rate-limited: {exc}")
                break
            except Exception as exc:
                logger.warning(f"[{self.name}] query failed: {exc}")

        if events:
            events = self._dedupe_by_title(events)
            self._events_cache = list(events)
            self._events_cache_time = now
            self._health.status = "healthy"
            self._health.event_count = len(events)
            return events

        if rate_limited and self._events_cache:
            return list(self._events_cache)

        self._health.event_count = 0
        return []

    def _enter_cooldown(self, retry_after_seconds: int | None = None) -> None:
        seconds = retry_after_seconds or 900
        self._cooldown_until = datetime.now(timezone.utc) + timedelta(seconds=max(seconds, 300))

    @staticmethod
    def _retry_after_seconds(resp: httpx.Response) -> int | None:
        value = resp.headers.get("Retry-After")
        if not value:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    async def _search_query(self, query_str, event_type, description, instruments):
        lookback = max(self.settings.news_lookback_hours, 6)

        # Build URL directly — GDELT is particular about encoding
        url = (
            f"{self.API_BASE}"
            f"?query={query_str} sourcelang:english"
            f"&mode=artlist"
            f"&maxrecords=20"
            f"&format=json"
            f"&timespan={lookback}h"
        )

        client = await self._get_client()
        resp = await client.get(url)
        if resp.status_code == 429:
            self._health.status = "failed"
            self._health.last_error = "GDELT rate-limited"
            raise GdeltRateLimitError(self._retry_after_seconds(resp))
        resp.raise_for_status()
        data = resp.json()
        self._health.status = "healthy"
        self._health.last_fetch_utc = datetime.now(timezone.utc)
        self._health.last_error = None

        if isinstance(data, dict):
            articles = data.get("articles", [])
        elif isinstance(data, list):
            articles = data
        else:
            return []

        if not articles:
            return []

        results: list[NormalisedEvent] = []
        now = datetime.now(timezone.utc)

        for article in articles:
            if not isinstance(article, dict):
                continue
            try:
                ev = self._normalise_article(article, event_type, description, instruments, now)
                if ev:
                    results.append(ev)
            except Exception as exc:
                logger.debug(f"[{self.name}] skip: {exc}")

        if len(results) >= 10:
            # Major cluster: 10+ articles = confirmed developing situation
            results[0].description = (
                f"MAJOR CLUSTER: {len(articles)} articles across global media. "
                + results[0].description
            )
            results[0].confidence = 0.85  # High confidence → near-full score
        elif len(results) >= 5:
            results[0].description = (
                f"CLUSTER: {len(articles)} articles detected. "
                + results[0].description
            )
            results[0].confidence = 0.75  # Moderate boost

        return results[:3]  # Max 3 per query to avoid flooding

    def _normalise_article(self, article, event_type, description, instruments, now):
        title = article.get("title", "")
        if not title or len(title) < 20:
            return None

        url = article.get("url", "")
        domain = article.get("domain", "")
        seendate = article.get("seendate", "")
        language = article.get("language", "English")
        source_country = article.get("sourcecountry", "")

        start_utc = self._parse_gdelt_date(seendate)
        if not start_utc:
            return None

        age_hours = (now - start_utc).total_seconds() / 3600
        if age_hours > self.settings.news_lookback_hours * 2:
            return None

        eid = hashlib.md5(f"geo-{title[:50]}-{seendate}".encode()).hexdigest()[:12]

        return NormalisedEvent(
            id=f"geo-{eid}",
            title=title[:200],
            category=EventCategory.CROSS_ASSET,
            event_type=event_type,
            classification=Classification.BREAKING,
            asset_class=AssetClass.BOTH,
            start_time_utc=start_utc,
            impact_score=0,
            caution_level="Low",
            suggested_action="Technicals usable",
            confidence=0.45,  # Individual article — discounted by scoring engine
            source_name=domain or self.name,
            source_url=url,
            affected_instruments=instruments,
            description=f"{description} Source: {domain}, Lang: {language}, Country: {source_country}",
        )

    @staticmethod
    def _parse_gdelt_date(date_str):
        if not date_str:
            return None
        ds = date_str.strip().replace(" ", "")
        for fmt in ["%Y%m%dT%H%M%SZ", "%Y%m%dT%H%M%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S", "%Y%m%d%H%M%S"]:
            try:
                dt = datetime.strptime(ds, fmt)
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    @staticmethod
    def _dedupe_by_title(events):
        seen = {}
        for ev in events:
            key = ev.title.lower().strip()[:40]
            if key not in seen:
                seen[key] = ev
        return list(seen.values())
