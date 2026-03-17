"""US Treasury upcoming auctions adapter (free, no key needed).

Uses the Fiscal Data API: https://api.fiscaldata.treasury.gov
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

# Security types that move rates markets
HIGH_IMPACT_TERMS = {"10-Year", "20-Year", "30-Year", "7-Year", "5-Year"}
MODERATE_IMPACT_TERMS = {"2-Year", "3-Year"}


class TreasuryAuctionAdapter(BaseAdapter):
    """Fetches upcoming US Treasury auction dates from fiscaldata.treasury.gov."""

    name = "TreasuryAuctionAdapter"
    BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/upcoming_auctions"

    async def fetch_events(self) -> list[NormalisedEvent]:
        now = datetime.now(timezone.utc)

        params = {
            "sort": "-auction_date",
            "page[size]": "50",
            "format": "json",
        }

        data = await self._fetch_json(self.BASE_URL, params=params)
        if not data or not isinstance(data, dict):
            return []

        records = data.get("data", [])
        events: list[NormalisedEvent] = []

        for item in records:
            try:
                ev = self._normalise(item)
                if ev:
                    events.append(ev)
            except Exception as exc:
                logger.debug(f"[{self.name}] skip: {exc}")

        self._health.event_count = len(events)
        return events

    def _normalise(self, item: dict) -> NormalisedEvent | None:
        auction_date = item.get("auction_date") or ""
        if not auction_date:
            return None

        try:
            dt = datetime.strptime(auction_date, "%Y-%m-%d")
            # Treasury auction results are released at 1:00 PM Eastern Time.
            # During EDT (Mar–Nov): 1 PM ET = 17:00 UTC
            # During EST (Nov–Mar): 1 PM ET = 18:00 UTC
            # Use 17:00 UTC as default — correct for most of the year.
            # FRNs auction at 11:30 AM ET = 15:30 UTC.
            dt = dt.replace(hour=17, minute=0, tzinfo=timezone.utc)
        except Exception:
            return None

        security_type = item.get("security_type", "")
        security_term = item.get("security_term", "")
        offering_amt = item.get("offering_amt", "")
        cusip = item.get("cusip", "")

        if not security_term:
            return None

        # Skip short-term Bills and CMBs — they're routine and low-impact
        is_bill = security_type.lower() in ("bill", "cmb")
        if is_bill:
            return None

        # FRNs auction earlier
        is_frn = "frn" in security_type.lower() or "frn" in security_term.lower()
        if is_frn:
            dt = dt.replace(hour=15, minute=30)

        time_note = (
            "Auction time estimated at 1:00 PM ET (17:00 UTC). "
            "Exact time may vary; EST months shift to 18:00 UTC."
        )

        # Format the offering amount
        amt_display = self._format_amount(offering_amt)
        title = f"Treasury {security_term} {security_type} Auction"
        if amt_display:
            title += f" ({amt_display})"

        # Determine impact based on term
        event_type = "treasury_auction"
        is_high = any(t in security_term for t in HIGH_IMPACT_TERMS)
        is_moderate = any(t in security_term for t in MODERATE_IMPACT_TERMS)

        eid = hashlib.md5(f"tsy-{auction_date}-{security_term}-{cusip}".encode()).hexdigest()[:12]

        return NormalisedEvent(
            id=f"tsy-{eid}",
            title=title,
            category=EventCategory.MACRO,
            event_type=event_type,
            classification=Classification.SCHEDULED,
            asset_class=AssetClass.BOTH if is_high else AssetClass.EQUITIES,
            start_time_utc=dt,
            impact_score=0,
            caution_level="Low",
            suggested_action="Technicals usable",
            confidence=0.9,
            source_name=self.name,
            source_url="https://fiscaldata.treasury.gov/datasets/upcoming-auctions/",
            affected_instruments=["TLT", "IEF", "ES", "NQ", "DXY", "rates-sensitive assets"],
            description=f"US Treasury {security_term} {security_type} auction. CUSIP: {cusip}. "
                        + ("Long-duration auction — can move yields and equity risk premium." if is_high
                           else "Duration auction with moderate rates impact."),
            data_quality_notes=time_note,
        )

    @staticmethod
    def _format_amount(raw: str) -> str:
        """Format a raw offering amount string into a human-readable dollar value."""
        if not raw:
            return ""
        try:
            num = float(raw)
            if num >= 1_000_000_000:
                return f"${num / 1_000_000_000:.0f}B"
            elif num >= 1_000_000:
                return f"${num / 1_000_000:.0f}M"
            elif num >= 1_000:
                return f"${num / 1_000:.0f}K"
            else:
                return f"${num:.0f}"
        except (ValueError, TypeError):
            return ""
