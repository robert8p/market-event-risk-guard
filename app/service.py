"""
Service layer — orchestrates adapter fetching, scoring, caching, and dedup.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from app.config import get_settings
from app.models import (
    AssetClass,
    Classification,
    CautionLevel,
    NormalisedEvent,
    SourceHealth,
)
from app.scoring import compute_summary_verdict, score_event

# Adapters
from app.adapters.base import BaseAdapter
from app.adapters.tradingeconomics import TradingEconomicsAdapter
from app.adapters.fmp_earnings import FmpEarningsAdapter
from app.adapters.coinmarketcal import CoinMarketCalAdapter
from app.adapters.newsapi import NewsApiBreakingAdapter
from app.adapters.coinbase_status import CoinbaseStatusAdapter, CoinbaseExchangeStatusAdapter
from app.adapters.fed import FedCalendarAdapter, FedPressAdapter
from app.adapters.sec import SecNewsAdapter
from app.adapters.kraken_status import KrakenStatusAdapter
from app.adapters.treasury_auctions import TreasuryAuctionAdapter
from app.adapters.ecb import EcbCalendarAdapter
from app.adapters.binance_announcements import BinanceAnnouncementsAdapter

logger = logging.getLogger(__name__)


class EventService:
    """Central service that manages adapters, caching, and event delivery."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self._adapters: list[BaseAdapter] = []
        self._cache: list[NormalisedEvent] = []
        self._cache_time: Optional[datetime] = None
        self._source_health: dict[str, SourceHealth] = {}
        self._refresh_lock = asyncio.Lock()
        self._init_adapters()

    # ── Initialisation ───────────────────────────────────────────────────

    def _init_adapters(self) -> None:
        s = self.settings
        adapter_map: list[tuple[bool, type]] = [
            (s.enable_tradingeconomics_adapter, TradingEconomicsAdapter),
            (s.enable_fmp_earnings_adapter, FmpEarningsAdapter),
            (s.enable_coinmarketcal_adapter, CoinMarketCalAdapter),
            (s.enable_news_adapter, NewsApiBreakingAdapter),
            (s.enable_coinbase_status_adapter, CoinbaseStatusAdapter),
            (s.enable_coinbase_exchange_status_adapter, CoinbaseExchangeStatusAdapter),
            (s.enable_fed_calendar_adapter, FedCalendarAdapter),
            (s.enable_fed_press_adapter, FedPressAdapter),
            (s.enable_sec_news_adapter, SecNewsAdapter),
            (s.enable_kraken_status_adapter, KrakenStatusAdapter),
            (s.enable_treasury_auction_adapter, TreasuryAuctionAdapter),
            (s.enable_ecb_calendar_adapter, EcbCalendarAdapter),
            (s.enable_binance_announcements_adapter, BinanceAnnouncementsAdapter),
        ]
        for enabled, cls in adapter_map:
            adapter = cls()
            if not enabled:
                adapter._health.enabled = False
            self._adapters.append(adapter)
            self._source_health[adapter.name] = adapter._health

    # ── Fetch + cache ────────────────────────────────────────────────────

    async def get_events(
        self,
        hours: int | None = None,
        asset_class: str | None = None,
        category: str | None = None,
        severity: str | None = None,
        classification: str | None = None,
        force_refresh: bool = False,
    ) -> list[NormalisedEvent]:
        """Return upcoming events (now → now+hours) plus events with active caution windows."""
        await self._maybe_refresh(force_refresh)

        window_hours = hours or self.settings.default_window_hours
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(hours=window_hours)

        events = []
        floor = now - timedelta(hours=window_hours)
        for e in self._cache:
            # Include if event is in the future window (now → now+hours)
            if now <= e.start_time_utc <= cutoff:
                events.append(e)
            # Include past events whose caution window is still active
            elif e.start_time_utc <= now and e.start_time_utc >= floor and e.caution_window_end_utc and e.caution_window_end_utc > now:
                events.append(e)
            # Include ALL ongoing incidents regardless of start time —
            # a 10-hour-old exchange outage is still a risk
            elif e.classification == Classification.ONGOING and e.start_time_utc <= now:
                events.append(e)

        events = self._apply_filters(events, asset_class, category, severity, classification)
        events.sort(key=lambda e: (-e.impact_score, e.start_time_utc))
        return events[: self.settings.max_events]

    async def get_recent_events(
        self,
        hours: int = 12,
        asset_class: str | None = None,
        category: str | None = None,
        severity: str | None = None,
        classification: str | None = None,
    ) -> list[NormalisedEvent]:
        """Return past events for context (caution window expired)."""
        await self._maybe_refresh()

        now = datetime.now(timezone.utc)
        floor = now - timedelta(hours=hours)

        events = []
        for e in self._cache:
            if floor <= e.start_time_utc < now:
                # Only include if caution window has expired (not already in upcoming)
                if not e.caution_window_end_utc or e.caution_window_end_utc <= now:
                    if e.classification != Classification.ONGOING:
                        events.append(e)

        events = self._apply_filters(events, asset_class, category, severity, classification)
        events.sort(key=lambda e: (-e.impact_score, e.start_time_utc))
        return events[:50]

    @staticmethod
    def _apply_filters(
        events: list[NormalisedEvent],
        asset_class: str | None = None,
        category: str | None = None,
        severity: str | None = None,
        classification: str | None = None,
    ) -> list[NormalisedEvent]:
        if asset_class and asset_class != "all":
            events = [e for e in events if e.asset_class.value == asset_class or e.asset_class == AssetClass.BOTH]
        if category:
            events = [e for e in events if e.category.value == category]
        if severity:
            events = [e for e in events if e.caution_level.value == severity]
        if classification:
            events = [e for e in events if e.classification.value == classification]
        return events

    async def get_summary(
        self,
        hours: int | None = None,
        asset_class: str | None = None,
        category: str | None = None,
        severity: str | None = None,
        classification: str | None = None,
    ) -> dict:
        """Return the rule-based trading-risk summary using the same filters as the event list."""
        window_hours = hours or self.settings.default_window_hours
        events = await self.get_events(
            hours=window_hours,
            asset_class=asset_class,
            category=category,
            severity=severity,
            classification=classification,
        )
        return compute_summary_verdict(events, window_hours, self.get_source_health())

    def get_source_health(self) -> list[SourceHealth]:
        return list(self._source_health.values())

    def start_background_refresh(self) -> None:
        """Fire off the initial refresh as a background task.
        Called once at startup. /health reads whatever state exists —
        it never triggers a refresh itself. /api/events waits via the lock."""
        asyncio.create_task(self._background_initial_refresh())

    async def _background_initial_refresh(self) -> None:
        """Run the first refresh in the background."""
        try:
            await self._maybe_refresh(force=True)
            logger.info("Initial background refresh complete")
        except Exception as exc:
            logger.error(f"Initial background refresh failed: {exc}")

    @property
    def is_refreshing(self) -> bool:
        """True if a refresh is currently in progress."""
        return self._refresh_lock.locked()

    @property
    def has_refreshed(self) -> bool:
        """True if at least one refresh has completed."""
        return self._cache_time is not None

    # ── Internal refresh logic ───────────────────────────────────────────

    # Deferred adapters are optional heavy sources. None are deferred currently.
    _DEFERRED_ADAPTERS = frozenset()

    async def _maybe_refresh(self, force: bool = False) -> None:
        """Refresh the cache if stale.

        First refresh: always waits — we must never serve an empty "all clear".
        Subsequent refreshes: if a refresh is already running, serve from
        the existing (stale but real) cache rather than blocking.
        """
        if self._cache_time is not None and self._refresh_lock.locked():
            # Cache has real data and a refresh is already running —
            # serve stale data rather than waiting. The background refresh
            # will update the cache when it finishes.
            return
        async with self._refresh_lock:
            now = datetime.now(timezone.utc)
            if (
                not force
                and self._cache_time
                and (now - self._cache_time).total_seconds() < self.settings.cache_ttl_seconds
            ):
                return
            await self._refresh()

    async def _refresh(self) -> None:
        """Two-phase refresh: fast adapters first, then deferred adapters in background."""
        logger.info("Refreshing events from all enabled adapters...")

        fast_adapters = []
        deferred_adapters = []
        for adapter in self._adapters:
            if adapter._health.enabled:
                if adapter.name in self._DEFERRED_ADAPTERS:
                    deferred_adapters.append(adapter)
                else:
                    fast_adapters.append(adapter)

        # Phase 1: fast adapters — these return in <2s typically
        fast_events = await self._fetch_adapters(fast_adapters)
        self._apply_events(fast_events)
        logger.info(f"Fast refresh complete: {len(self._cache)} events from {len(fast_adapters)} adapters")

        # Phase 2: deferred adapters — run in background, merge when done
        if deferred_adapters:
            asyncio.create_task(self._deferred_refresh(deferred_adapters))

    async def _deferred_refresh(self, adapters: list) -> None:
        """Fetch slow adapters and merge into the existing cache."""
        try:
            new_events = await self._fetch_adapters(adapters)
            if new_events:
                all_events = list(self._cache) + [score_event(ev) for ev in new_events]
                self._cache = self._deduplicate(all_events)
                logger.info(f"Deferred refresh added {len(new_events)} events, cache now {len(self._cache)}")
        except Exception as exc:
            logger.error(f"Deferred refresh failed: {exc}")

    async def _fetch_adapters(self, adapters: list) -> list[NormalisedEvent]:
        """Run a list of adapters concurrently and return raw events."""
        tasks = [a.fetch_events() for a in adapters]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_events: list[NormalisedEvent] = []
        for adapter, result in zip(adapters, results):
            if isinstance(result, Exception):
                logger.error(f"[{adapter.name}] adapter failed: {result}")
                adapter._health.status = "failed"
                adapter._health.last_error = str(result)
                adapter._health.last_fetch_utc = datetime.now(timezone.utc)
            elif isinstance(result, list):
                adapter._health.event_count = len(result)
                all_events.extend(result)

            self._source_health[adapter.name] = adapter.health()

        return all_events

    def _apply_events(self, raw_events: list[NormalisedEvent]) -> None:
        """Score, dedup, and store events."""
        scored = []
        for ev in raw_events:
            try:
                scored.append(score_event(ev))
            except Exception as exc:
                logger.debug(f"Scoring failed for {ev.id}: {exc}")

        self._cache = self._deduplicate(scored)
        self._cache_time = datetime.now(timezone.utc)

    async def shutdown(self) -> None:
        """Close all adapter HTTP clients."""
        for adapter in self._adapters:
            try:
                await adapter.close()
            except Exception:
                pass

    @staticmethod
    def _deduplicate(events: list[NormalisedEvent]) -> list[NormalisedEvent]:
        """Remove near-duplicate events using multi-tier matching.

        Tier 1: Same event_type + same date + overlapping entity/instrument
                (catches "AAPL Earnings" from FMP and "AAPL earnings report" from news)
        Tier 2: Normalised title similarity + same hour
                (catches the same headline from different news sources)

        Always keeps the higher-scored version. Never merges events with
        different classifications (scheduled vs breaking vs ongoing).
        """
        seen: dict[str, NormalisedEvent] = {}

        for ev in events:
            # ── Tier 1: structural key ───────────────────────────────────
            # For events with known structure (earnings, auctions, etc.),
            # build a key from type + date + primary entity
            structural_key = _structural_dedup_key(ev)

            if structural_key and structural_key in seen:
                if ev.impact_score > seen[structural_key].impact_score:
                    seen[structural_key] = ev
                continue
            elif structural_key:
                seen[structural_key] = ev
                continue

            # ── Tier 2: title similarity ─────────────────────────────────
            # Strip common prefixes (source names), normalise, truncate
            clean_title = _normalise_title(ev.title)
            key_time = ev.start_time_utc.strftime("%Y%m%d%H")
            title_key = f"t|{clean_title}|{key_time}|{ev.classification.value}"

            if title_key in seen:
                if ev.impact_score > seen[title_key].impact_score:
                    seen[title_key] = ev
            else:
                seen[title_key] = ev

        return list(seen.values())


def _structural_dedup_key(ev: NormalisedEvent) -> str | None:
    """Build a structural dedup key for events with known structure."""
    date_str = ev.start_time_utc.strftime("%Y%m%d")

    # Earnings: type + date + ticker (resolved from title or company name)
    if ev.event_type in ("earnings", "mega_cap_earnings"):
        ticker = _extract_ticker(ev.title)
        if ticker:
            return f"earn|{date_str}|{ticker}"
        return None  # Can't identify the company — skip structural dedup

    # Treasury auctions: type + date + term
    if ev.event_type == "treasury_auction":
        # Extract term from title (e.g. "Treasury 10-Year Note Auction")
        for term in ["30-year", "20-year", "10-year", "7-year", "5-year", "3-year", "2-year"]:
            if term in ev.title.lower():
                return f"tsy|{date_str}|{term}"
        return f"tsy|{date_str}|{ev.title[:30].lower()}"

    # Exchange incidents: source + id
    if ev.event_type in ("exchange_outage", "exchange_maintenance") and ev.source_name:
        return f"exch|{ev.source_name}|{ev.id}"

    # Fed/ECB events: type + date
    if ev.event_type in ("fomc", "rate_decision", "ecb_rate", "central_bank_minutes", "central_bank_speech"):
        return f"cb|{date_str}|{ev.event_type}"

    return None


def _normalise_title(title: str) -> str:
    """Strip source prefixes and normalise a title for comparison."""
    t = title.lower().strip()
    # Strip common source prefixes
    for prefix in ["sec: ", "fed: ", "fed press: ", "ecb: ", "coinbase: ",
                    "coinbase exchange: ", "kraken: ", "binance: "]:
        if t.startswith(prefix):
            t = t[len(prefix):]
            break
    # Remove punctuation, collapse whitespace
    t = "".join(c if c.isalnum() or c == " " else "" for c in t)
    t = " ".join(t.split())
    return t[:50]


# ── Company name → ticker mapping for cross-source dedup ─────────────────────
# Covers the ~30 mega-caps that are most likely to appear in both FMP (by ticker)
# and news sources (by company name). Ordered longest-first so "Alphabet Inc"
# matches before "Alpha".

_COMPANY_TO_TICKER: list[tuple[str, str]] = [
    ("alphabet", "googl"), ("google", "googl"),
    ("apple", "aapl"),
    ("microsoft", "msft"),
    ("amazon", "amzn"),
    ("nvidia", "nvda"),
    ("meta platforms", "meta"), ("facebook", "meta"),
    ("tesla", "tsla"),
    ("berkshire hathaway", "brk"),
    ("jpmorgan", "jpm"), ("jp morgan", "jpm"),
    ("johnson & johnson", "jnj"), ("johnson and johnson", "jnj"),
    ("unitedhealth", "unh"),
    ("visa", "v"),
    ("mastercard", "ma"),
    ("procter & gamble", "pg"), ("procter and gamble", "pg"),
    ("home depot", "hd"),
    ("exxon mobil", "xom"), ("exxonmobil", "xom"),
    ("chevron", "cvx"),
    ("eli lilly", "lly"),
    ("costco", "cost"),
    ("abbvie", "abbv"),
    ("walmart", "wmt"),
    ("merck", "mrk"),
    ("broadcom", "avgo"),
    ("salesforce", "crm"),
    ("netflix", "nflx"),
    ("adobe", "adbe"),
    ("intel", "intc"),
    ("qualcomm", "qcom"),
    ("oracle", "orcl"),
    ("amd", "amd"), ("advanced micro", "amd"),
]


def _extract_ticker(title: str) -> str | None:
    """Extract a normalised ticker symbol from an earnings event title.

    Tries two strategies:
    1. If the first word is all-caps and <=5 chars, treat it as a ticker (FMP style: "AAPL Earnings")
    2. Otherwise, search for known company names in the title (news style: "Apple Inc. earnings")
    """
    t = title.strip()
    parts = t.split()
    if not parts:
        return None

    # Strategy 1: first word looks like a ticker (AAPL, MSFT, NVDA)
    first = parts[0].rstrip(".,;:!?")
    if first.isupper() and 1 <= len(first) <= 5 and first.isalpha():
        return first.lower()

    # Strategy 2: search for known company names
    t_lower = t.lower()
    for name, ticker in _COMPANY_TO_TICKER:
        if name in t_lower:
            return ticker

    return None
