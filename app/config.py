"""Centralised configuration loaded from environment variables."""

from __future__ import annotations

import os
from functools import lru_cache

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    # ── App ──────────────────────────────────────
    app_name: str = Field(default="Market Event Risk Guard v2.4.0")
    app_env: str = Field(default="production")
    app_timezone: str = Field(default="Europe/London")
    default_window_hours: int = Field(default=8)
    refresh_interval_seconds: int = Field(default=60)
    cache_ttl_seconds: int = Field(default=60)
    http_timeout_seconds: int = Field(default=15)
    max_events: int = Field(default=200)
    news_lookback_hours: int = Field(default=12)

    # ── Adapter toggles ─────────────────────────
    enable_tradingeconomics_adapter: bool = Field(default=True)
    enable_fmp_earnings_adapter: bool = Field(default=True)
    enable_coinmarketcal_adapter: bool = Field(default=True)
    enable_news_adapter: bool = Field(default=True)
    enable_coinbase_status_adapter: bool = Field(default=True)
    enable_coinbase_exchange_status_adapter: bool = Field(default=True)
    enable_fed_calendar_adapter: bool = Field(default=True)
    enable_sec_news_adapter: bool = Field(default=True)
    enable_fed_press_adapter: bool = Field(default=True)
    enable_kraken_status_adapter: bool = Field(default=True)
    enable_treasury_auction_adapter: bool = Field(default=True)
    enable_ecb_calendar_adapter: bool = Field(default=True)
    enable_binance_announcements_adapter: bool = Field(default=True)

    # ── TradingEconomics ─────────────────────────
    tradingeconomics_base_url: str = Field(default="https://api.tradingeconomics.com")
    tradingeconomics_client_key: str = Field(default="")
    tradingeconomics_client_secret: str = Field(default="")

    # ── FMP ───────────────────────────────────────
    fmp_base_url: str = Field(default="https://financialmodelingprep.com/stable")
    fmp_api_key: str = Field(default="")

    # ── CoinMarketCal ─────────────────────────────
    coinmarketcal_base_url: str = Field(default="https://developers.coinmarketcal.com/v1")
    coinmarketcal_api_key: str = Field(default="")

    # ── NewsAPI ───────────────────────────────────
    newsapi_base_url: str = Field(default="https://newsapi.org/v2")
    newsapi_key: str = Field(default="")
    breaking_news_query: str = Field(
        default="(Fed OR FOMC OR CPI OR inflation OR payrolls OR tariffs OR sanctions OR geopolitical OR SEC OR ETF OR bitcoin OR ethereum OR stablecoin OR exchange outage OR listing OR delisting)"
    )

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}


@lru_cache()
def get_settings() -> Settings:
    return Settings()
