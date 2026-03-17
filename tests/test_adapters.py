"""Tests for adapter normalisation logic (no network)."""
from datetime import datetime, timezone, timedelta
from app.adapters.tradingeconomics import TradingEconomicsAdapter
from app.adapters.fmp_earnings import FmpEarningsAdapter
from app.adapters.newsapi import NewsApiBreakingAdapter
from app.adapters.coinbase_status import CoinbaseStatusAdapter
from app.adapters.sec import SecNewsAdapter

class TestTradingEconomics:
    def setup_method(self):
        self.a = TradingEconomicsAdapter()

    def test_normalise_cpi(self):
        item = {"Event":"CPI YoY","Date":(datetime.now(timezone.utc)+timedelta(hours=2)).isoformat(),"Importance":3,"Country":"United States"}
        ev = self.a._normalise(item)
        assert ev is not None
        assert ev.event_type == "cpi"

    def test_skip_empty(self):
        assert self.a._normalise({}) is None

    def test_classify_fomc(self):
        assert TradingEconomicsAdapter._classify_event_type("FOMC Meeting") == "fomc"

    def test_classify_nfp(self):
        assert TradingEconomicsAdapter._classify_event_type("Nonfarm Payrolls") == "nonfarm_payrolls"

class TestFmpEarnings:
    def setup_method(self):
        self.a = FmpEarningsAdapter()

    def test_mega_cap(self):
        ev = self.a._normalise({"symbol":"AAPL","date":"2026-03-20","time":"amc"})
        assert ev is not None
        assert ev.event_type == "mega_cap_earnings"

    def test_regular(self):
        ev = self.a._normalise({"symbol":"ACME","date":"2026-03-20"})
        assert ev is not None
        assert ev.event_type == "earnings"

class TestNewsApiClassify:
    def test_fed(self):
        et,_,_ = NewsApiBreakingAdapter._classify("the fed raised rates")
        assert et == "fomc"

    def test_tariff(self):
        et,_,_ = NewsApiBreakingAdapter._classify("new tariff on imports")
        assert et == "tariffs"

    def test_unknown(self):
        et,_,_ = NewsApiBreakingAdapter._classify("random headline about cats")
        assert et == "breaking_news"

class TestCoinbase:
    def setup_method(self):
        self.a = CoinbaseStatusAdapter()

    def test_resolved_skipped(self):
        assert self.a._normalise({"name":"Test","status":"resolved","created_at":"2026-03-16T10:00:00Z"}) is None

    def test_active(self):
        ev = self.a._normalise({"name":"Degraded","status":"investigating","created_at":"2026-03-16T10:00:00Z","id":"x"})
        assert ev is not None

class TestSecClassify:
    def test_crypto(self):
        et, ac = SecNewsAdapter._classify("sec charges crypto exchange")
        assert et == "crypto_regulatory"

    def test_enforcement(self):
        et, ac = SecNewsAdapter._classify("enforcement action fraud")
        assert et == "sec_action"
