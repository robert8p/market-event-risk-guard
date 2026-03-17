"""CoinMarketCal crypto event calendar adapter."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone

from app.adapters.base import BaseAdapter
from app.models import (
    AssetClass,
    Classification,
    EventCategory,
    NormalisedEvent,
)

logger = logging.getLogger(__name__)

CRYPTO_EVENT_TYPES = {
    "hard_fork": "hard_fork",
    "swap": "protocol_upgrade",
    "upgrade": "protocol_upgrade",
    "listing": "listing",
    "airdrop": "token_unlock",
    "token_burn": "governance",
    "partnership": "governance",
    "release": "protocol_upgrade",
    "conference": "governance",
    "brand": "governance",
    "regulation": "crypto_regulatory",
    "etf": "etf_decision",
    "stablecoin": "stablecoin_depeg",
    "exchange": "exchange_outage",
    "halving": "hard_fork",
    "mainnet": "protocol_upgrade",
    "testnet": "protocol_upgrade",
    "lock": "token_unlock",
    "unlock": "token_unlock",
    "burn": "governance",
}


class CoinMarketCalAdapter(BaseAdapter):
    name = "CoinMarketCalAdapter"

    async def fetch_events(self) -> list[NormalisedEvent]:
        s = self.settings
        if not self._is_valid_key(s.coinmarketcal_api_key):
            logger.info(f"[{self.name}] No API key configured, skipping.")
            self._health.status = "needs_key"
            self._health.last_error = "No API key"
            return []

        url = f"{s.coinmarketcal_base_url}/events"
        headers = {
            "x-api-key": s.coinmarketcal_api_key,
            "Accept": "application/json",
        }
        params = {
            "max": 75,
            "sortBy": "date_event",
        }

        data = await self._fetch_json(url, params=params, headers=headers)
        if not data:
            return []

        body = data if isinstance(data, list) else data.get("body", data.get("data", []))
        if not isinstance(body, list):
            return []

        events: list[NormalisedEvent] = []
        for item in body:
            try:
                ev = self._normalise(item)
                if ev:
                    events.append(ev)
            except Exception as exc:
                logger.debug(f"[{self.name}] skip item: {exc}")

        self._health.event_count = len(events)
        return events

    def _normalise(self, item: dict) -> NormalisedEvent | None:
        title_obj = item.get("title", {})
        title = title_obj if isinstance(title_obj, str) else title_obj.get("en", str(title_obj))
        if not title:
            return None

        date_str = item.get("date_event") or ""
        if not date_str:
            return None

        try:
            start_utc = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            if start_utc.tzinfo is None:
                start_utc = start_utc.replace(tzinfo=timezone.utc)
        except Exception:
            return None

        # Coins
        coins = item.get("coins", [])
        coin_symbols = [c.get("symbol", "") for c in coins if isinstance(c, dict)] if isinstance(coins, list) else []
        instruments = [f"{sym}-USD" for sym in coin_symbols if sym] or ["BTC-USD", "ETH-USD"]

        # Classify
        categories = item.get("categories", [])
        cat_names = []
        if isinstance(categories, list):
            for c in categories:
                if isinstance(c, dict):
                    cat_names.append(c.get("name", "").lower())
                elif isinstance(c, str):
                    cat_names.append(c.lower())

        event_type = self._classify(title, cat_names)

        eid = hashlib.md5(f"cmc-{title}-{date_str}".encode()).hexdigest()[:12]

        return NormalisedEvent(
            id=f"cmc-{eid}",
            title=title,
            category=EventCategory.CRYPTO,
            event_type=event_type,
            classification=Classification.SCHEDULED,
            asset_class=AssetClass.CRYPTO,
            start_time_utc=start_utc,
            impact_score=0,
            caution_level="Low",
            suggested_action="Technicals usable",
            confidence=0.5,
            source_name=self.name,
            source_url=item.get("source", "https://coinmarketcal.com"),
            affected_instruments=instruments,
            description=item.get("description", ""),
        )

    @staticmethod
    def _classify(title: str, cat_names: list[str]) -> str:
        combined = (title + " " + " ".join(cat_names)).lower()
        for keyword, etype in CRYPTO_EVENT_TYPES.items():
            if keyword in combined:
                return etype
        return "crypto_event"
