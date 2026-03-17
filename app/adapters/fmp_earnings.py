"""Financial Modeling Prep earnings calendar adapter."""

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

# Mega-cap / index-moving tickers
MEGA_CAPS = {
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA",
    "BRK-A", "BRK-B", "JPM", "V", "UNH", "MA", "HD", "PG", "JNJ",
    "AVGO", "XOM", "CVX", "LLY", "COST", "ABBV", "WMT", "MRK",
    "CRM", "NFLX", "AMD", "ORCL", "ADBE", "INTC", "QCOM",
}


class FmpEarningsAdapter(BaseAdapter):
    name = "FmpEarningsAdapter"

    async def fetch_events(self) -> list[NormalisedEvent]:
        s = self.settings
        if not self._is_valid_key(s.fmp_api_key):
            logger.info(f"[{self.name}] No API key configured, skipping.")
            self._health.status = "needs_key"
            self._health.last_error = "No API key"
            return []

        now = datetime.now(timezone.utc)
        from_date = now.strftime("%Y-%m-%d")
        to_date = (now + timedelta(days=3)).strftime("%Y-%m-%d")

        url = f"{s.fmp_base_url}/earning-calendar"
        params = {
            "from": from_date,
            "to": to_date,
            "apikey": s.fmp_api_key,
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
        symbol = item.get("symbol") or ""
        if not symbol:
            return None

        date_str = item.get("date") or ""
        if not date_str:
            return None

        try:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            # FMP provides "bmo" (before market open) or "amc" (after market close).
            # Exact times are not provided — these are best-effort estimates
            # based on typical US market earnings release patterns.
            time_label = item.get("time", "bmo")
            if time_label == "amc":
                # After market close: most companies report at ~4:05 PM ET = 20:05 UTC
                dt = dt.replace(hour=20, minute=5, tzinfo=timezone.utc)
                time_note = "Estimated AMC (after market close, ~4:05 PM ET). Exact time not provided by source."
            elif time_label == "bmo":
                # Before market open: most companies report at ~7:00 AM ET = 11:00 UTC
                # Pre-market reaction can begin from 4:00 AM ET = 08:00 UTC
                dt = dt.replace(hour=11, minute=0, tzinfo=timezone.utc)
                time_note = "Estimated BMO (before market open, ~7:00 AM ET). Exact time not provided by source."
            else:
                # Unknown timing — assume BMO as the conservative default
                dt = dt.replace(hour=11, minute=0, tzinfo=timezone.utc)
                time_note = "Timing unknown — assumed before market open. Verify exact time before trading."
        except Exception:
            return None

        is_mega = symbol.upper() in MEGA_CAPS
        event_type = "mega_cap_earnings" if is_mega else "earnings"

        eid = hashlib.md5(f"fmp-{symbol}-{date_str}".encode()).hexdigest()[:12]

        instruments = ["ES", "NQ", "SPY", "QQQ", symbol]
        if is_mega:
            instruments.append("mega-cap tech")

        return NormalisedEvent(
            id=f"fmp-{eid}",
            title=f"{symbol} Earnings",
            category=EventCategory.EQUITY,
            event_type=event_type,
            classification=Classification.SCHEDULED,
            asset_class=AssetClass.EQUITIES,
            start_time_utc=dt,
            impact_score=0,
            caution_level="Low",
            suggested_action="Technicals usable",
            confidence=0.7,
            source_name=self.name,
            source_url=f"https://financialmodelingprep.com/financial-statements/{symbol}",
            affected_instruments=instruments,
            description=f"{symbol} scheduled earnings report. {'Mega-cap with high index weight.' if is_mega else ''}",
            data_quality_notes=time_note,
        )
