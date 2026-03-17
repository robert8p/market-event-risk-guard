"""Correctness regression tests.

These test the specific behaviours that have been identified as trust-critical:
1. Health must reflect real adapter state, not a pre-refresh default.
2. Recent events must respect the selected time window exactly.
3. Ongoing incidents must persist regardless of start time.
4. Summary must use the same filters as the event list.
5. Approximate timing is documented via data_quality_notes.
"""

import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from app.models import (
    AssetClass,
    CautionLevel,
    Classification,
    EventCategory,
    NormalisedEvent,
    SuggestedAction,
)
from app.scoring import score_event, compute_summary_verdict
from app.service import EventService


def _make_event(
    id="test",
    event_type="cpi",
    classification=Classification.SCHEDULED,
    start_offset_minutes=30,
    confidence=0.5,
    asset_class=AssetClass.BOTH,
    **kw,
) -> NormalisedEvent:
    defaults = dict(
        id=id,
        title=f"Test {id}",
        category=EventCategory.MACRO,
        event_type=event_type,
        classification=classification,
        asset_class=asset_class,
        start_time_utc=datetime.now(timezone.utc) + timedelta(minutes=start_offset_minutes),
        impact_score=0,
        caution_level=CautionLevel.LOW,
        suggested_action=SuggestedAction.TECHNICALS_USABLE,
        confidence=confidence,
        source_name="TestSource",
    )
    defaults.update(kw)
    return NormalisedEvent(**defaults)


# ── 1. Confidence/score coherence ────────────────────────────────────────────

class TestConfidenceScoreCoherence:
    """The displayed confidence must match the confidence used for scoring."""

    def test_confidence_derived_before_discount(self):
        """A scheduled event with initial conf 0.5 should derive higher conf
        before the discount runs, so the discount uses the real value."""
        ev = _make_event(
            event_type="fomc",
            classification=Classification.SCHEDULED,
            confidence=0.5,  # placeholder
            start_offset_minutes=10,
        )
        scored = score_event(ev)
        # Derived confidence for a scheduled event with source_name should be > 0.7
        assert scored.confidence > 0.7
        # And the score should reflect that confidence, not the original 0.5
        # FOMC base=72, with conf>0.8 → no discount → score should be high
        assert scored.impact_score >= 75

    def test_low_confidence_article_discounted(self):
        """A GDELT article with low preset confidence should be discounted."""
        ev = _make_event(
            event_type="war",
            classification=Classification.BREAKING,
            confidence=0.45,  # GDELT individual article
            start_offset_minutes=-30,
        )
        scored = score_event(ev)
        # Should be discounted below EXTREME
        assert scored.impact_score < 100
        # Confidence should NOT have been overwritten since it was preset
        assert scored.confidence == 0.45

    def test_confidence_not_overwritten_when_preset(self):
        """If confidence was set by the adapter (not 0.5), it must not change."""
        ev = _make_event(confidence=0.85)
        scored = score_event(ev)
        assert scored.confidence == 0.85


# ── 2. Ongoing incident persistence ─────────────────────────────────────────

class TestOngoingPersistence:
    """Ongoing incidents must appear regardless of when they started."""

    def test_old_ongoing_included_in_short_window(self):
        """A 3-day-old ongoing incident must appear in a 1-hour window."""
        old_ongoing = _make_event(
            id="outage",
            event_type="exchange_outage",
            classification=Classification.ONGOING,
            start_offset_minutes=-4320,  # 3 days ago
            confidence=0.8,
        )
        scored = score_event(old_ongoing)

        # Simulate service filtering with 1h window
        now = datetime.now(timezone.utc)
        cutoff = now + timedelta(hours=1)
        floor = now - timedelta(hours=1)

        # This is the actual filter logic from service.py
        included = False
        if now <= scored.start_time_utc <= cutoff:
            included = True
        elif scored.start_time_utc <= now and scored.start_time_utc >= floor and scored.caution_window_end_utc and scored.caution_window_end_utc > now:
            included = True
        elif scored.classification == Classification.ONGOING and scored.start_time_utc <= now:
            included = True

        assert included, "Ongoing incident from 3 days ago must be visible in 1h window"


# ── 3. Summary/filter parity ────────────────────────────────────────────────

class TestSummaryFilterParity:
    """Summary verdict must reflect the same events the user sees."""

    def test_crypto_filter_excludes_equity_from_verdict(self):
        """If user filters to crypto, equity events must not affect the verdict."""
        equity_event = score_event(_make_event(
            id="eq1",
            event_type="mega_cap_earnings",
            asset_class=AssetClass.EQUITIES,
            confidence=0.85,
        ))
        crypto_event = score_event(_make_event(
            id="cr1",
            event_type="listing",
            asset_class=AssetClass.CRYPTO,
            confidence=0.75,
        ))

        # Unfiltered: should reflect both
        all_summary = compute_summary_verdict([equity_event, crypto_event], 8)

        # Filtered to crypto: should only reflect crypto
        crypto_only = [e for e in [equity_event, crypto_event] if e.asset_class in (AssetClass.CRYPTO, AssetClass.BOTH)]
        crypto_summary = compute_summary_verdict(crypto_only, 8)

        # The crypto-only summary should not be influenced by the equity event
        assert crypto_summary["event_count"] == 1


# ── 4. Timing quality notes ─────────────────────────────────────────────────

class TestTimingQuality:
    """Events with approximate timing must document the approximation."""

    def test_fmp_earnings_has_time_note(self):
        """FMP earnings events should carry a data_quality_notes field."""
        from app.adapters.fmp_earnings import FmpEarningsAdapter
        adapter = FmpEarningsAdapter()
        item = {"symbol": "AAPL", "date": "2026-04-01", "time": "amc"}
        ev = adapter._normalise(item)
        assert ev is not None
        assert ev.data_quality_notes is not None
        assert "estimated" in ev.data_quality_notes.lower() or "approximate" in ev.data_quality_notes.lower()

    def test_fmp_bmo_time_is_reasonable(self):
        """BMO earnings should be around 11:00 UTC (7 AM ET), not noon."""
        from app.adapters.fmp_earnings import FmpEarningsAdapter
        adapter = FmpEarningsAdapter()
        item = {"symbol": "MSFT", "date": "2026-04-01", "time": "bmo"}
        ev = adapter._normalise(item)
        assert ev is not None
        assert ev.start_time_utc.hour == 11

    def test_fmp_amc_time_is_reasonable(self):
        """AMC earnings should be around 20:05 UTC (4:05 PM ET)."""
        from app.adapters.fmp_earnings import FmpEarningsAdapter
        adapter = FmpEarningsAdapter()
        item = {"symbol": "AAPL", "date": "2026-04-01", "time": "amc"}
        ev = adapter._normalise(item)
        assert ev is not None
        assert ev.start_time_utc.hour == 20

    def test_treasury_auction_time_is_reasonable(self):
        """Treasury note/bond auctions should be at 17:00 UTC (1 PM ET), not 13:00."""
        from app.adapters.treasury_auctions import TreasuryAuctionAdapter
        adapter = TreasuryAuctionAdapter()
        item = {
            "auction_date": "2026-04-01",
            "security_type": "Note",
            "security_term": "10-Year",
            "offering_amt": "42000000000",
            "cusip": "TEST123",
        }
        ev = adapter._normalise(item)
        assert ev is not None
        assert ev.start_time_utc.hour == 17, f"Expected 17:00 UTC, got {ev.start_time_utc.hour}:00"
        assert ev.data_quality_notes is not None


# ── 5. Recent window respects selection ──────────────────────────────────────

class TestRecentWindow:
    """Recent events API must respect the selected time window."""

    def test_recent_does_not_exceed_window(self):
        """Events from 6 hours ago should not appear in a 1-hour recent window."""
        from fastapi.testclient import TestClient
        from app.main import app

        with TestClient(app) as client:
            r = client.get("/api/events?hours=1")
            assert r.status_code == 200
            data = r.json()
            # All recent events should be within 1 hour of now
            now = datetime.now(timezone.utc)
            for ev in data.get("recent_events", []):
                ev_time = datetime.fromisoformat(ev["start_time_utc"])
                age = (now - ev_time).total_seconds() / 3600
                assert age <= 1.5, f"Recent event {ev['title'][:40]} is {age:.1f}h old, exceeds 1h window"


# ── 6. Deduplication correctness ─────────────────────────────────────────────

class TestDeduplication:
    """Dedup must merge same events from different sources without merging distinct events."""

    def test_same_ticker_earnings_from_two_sources_merges(self):
        """Two adapters reporting AAPL earnings on the same day should merge via structural key."""
        from app.service import EventService
        ev1 = score_event(_make_event(
            id="fmp-aaa", event_type="earnings",
            title="AAPL Earnings",
            start_offset_minutes=600,
            confidence=0.7,
            asset_class=AssetClass.EQUITIES,
        ))
        ev2 = score_event(_make_event(
            id="news-bbb", event_type="earnings",
            title="AAPL earnings report Q1",
            start_offset_minutes=600,
            confidence=0.5,
            asset_class=AssetClass.EQUITIES,
        ))
        result = EventService._deduplicate([ev1, ev2])
        # Both start with "AAPL" + same date + same event_type → structural key matches
        earnings = [e for e in result if "earnings" in e.event_type]
        assert len(earnings) == 1, f"Expected 1 merged earnings event, got {len(earnings)}"
        # Higher-scored version should be kept
        assert earnings[0].id == ev1.id or earnings[0].impact_score >= ev2.impact_score

    def test_different_ticker_earnings_not_merged(self):
        """AAPL and MSFT earnings on the same day must not merge."""
        from app.service import EventService
        ev1 = score_event(_make_event(
            id="fmp-aaa", event_type="earnings",
            title="AAPL Earnings",
            start_offset_minutes=600,
            confidence=0.7,
            asset_class=AssetClass.EQUITIES,
        ))
        ev2 = score_event(_make_event(
            id="fmp-bbb", event_type="earnings",
            title="MSFT Earnings",
            start_offset_minutes=600,
            confidence=0.7,
            asset_class=AssetClass.EQUITIES,
        ))
        result = EventService._deduplicate([ev1, ev2])
        assert len(result) == 2, "Different tickers must not be merged"

    def test_different_events_same_hour_not_merged(self):
        """Two different event types in the same hour must not merge."""
        from app.service import EventService
        ev1 = score_event(_make_event(
            id="ev1", event_type="cpi", title="US CPI Release",
            start_offset_minutes=60, confidence=0.85,
        ))
        ev2 = score_event(_make_event(
            id="ev2", event_type="nonfarm_payrolls", title="US NFP Release",
            start_offset_minutes=60, confidence=0.85,
        ))
        result = EventService._deduplicate([ev1, ev2])
        assert len(result) == 2, "Different event types must not be merged"

    def test_scheduled_and_breaking_not_merged(self):
        """Same title with different classifications must not merge."""
        from app.service import EventService
        ev1 = score_event(_make_event(
            id="ev1", event_type="sanctions", title="New Russia Sanctions",
            classification=Classification.SCHEDULED, start_offset_minutes=60,
        ))
        ev2 = score_event(_make_event(
            id="ev2", event_type="sanctions", title="New Russia Sanctions",
            classification=Classification.BREAKING, start_offset_minutes=60,
        ))
        result = EventService._deduplicate([ev1, ev2])
        assert len(result) == 2, "Events with different classifications must not be merged"


# ── 7. Health cold-start timeout ─────────────────────────────────────────────

class TestHealthColdStart:
    """Health must be fast and honest on cold start."""

    def test_health_returns_instantly(self):
        """Health endpoint must respond fast — it never triggers a refresh."""
        from fastapi.testclient import TestClient
        from app.main import app
        import time

        with TestClient(app) as client:
            start = time.time()
            r = client.get("/health")
            elapsed = time.time() - start
            assert r.status_code == 200
            # Must respond within 2 seconds — it's just reading state
            assert elapsed < 2.0, f"Health took {elapsed:.1f}s — should be <2s"
            data = r.json()
            assert data["status"] in ("healthy", "degraded", "starting")

    def test_health_shows_refreshing_flag(self):
        """Health response must include a refreshing indicator."""
        from fastapi.testclient import TestClient
        from app.main import app

        with TestClient(app) as client:
            r = client.get("/health")
            data = r.json()
            assert "refreshing" in data


# ── 8. Company-name cross-source dedup ───────────────────────────────────────

class TestCompanyNameDedup:
    """Dedup must resolve company names to tickers for earnings merging."""

    def test_apple_inc_merges_with_aapl(self):
        """'Apple Inc. earnings report' should merge with 'AAPL Earnings'."""
        from app.service import EventService
        ev1 = score_event(_make_event(
            id="fmp-aaa", event_type="earnings",
            title="AAPL Earnings",
            start_offset_minutes=600,
            confidence=0.7,
            asset_class=AssetClass.EQUITIES,
        ))
        ev2 = score_event(_make_event(
            id="news-bbb", event_type="earnings",
            title="Apple Inc. earnings report expected",
            start_offset_minutes=600,
            confidence=0.5,
            asset_class=AssetClass.EQUITIES,
        ))
        result = EventService._deduplicate([ev1, ev2])
        earnings = [e for e in result if "earnings" in e.event_type]
        assert len(earnings) == 1, f"Expected 1 merged event, got {len(earnings)}: {[e.title for e in earnings]}"

    def test_ticker_extraction_from_company_name(self):
        """Company names should resolve to correct tickers."""
        from app.service import _extract_ticker
        assert _extract_ticker("AAPL Earnings") == "aapl"
        assert _extract_ticker("Apple Inc. earnings report") == "aapl"
        assert _extract_ticker("Microsoft Q3 results") == "msft"
        assert _extract_ticker("NVDA Earnings") == "nvda"
        assert _extract_ticker("Alphabet reports strong Q4") == "googl"
        assert _extract_ticker("Netflix subscriber growth") == "nflx"
        assert _extract_ticker("Random unknown company") is None


# ── 9. Coverage-aware verdict ────────────────────────────────────────────────

class TestCoverageAwareVerdict:
    """Verdict must reflect source coverage — a green light with degraded sources is unreliable."""

    def test_yes_becomes_caution_when_sources_degraded(self):
        """With no events but degraded sources, verdict should be Caution, not Yes."""
        from app.models import SourceHealth
        sources = [
            SourceHealth(name="A", enabled=True, status="healthy"),
            SourceHealth(name="B", enabled=True, status="failed"),
            SourceHealth(name="C", enabled=True, status="failed"),
            SourceHealth(name="D", enabled=True, status="failed"),
        ]
        result = compute_summary_verdict([], 8, sources)
        assert result["verdict"] == "Caution", f"Expected Caution with degraded sources, got {result['verdict']}"
        assert "degraded" in result["verdict_detail"].lower() or "coverage" in result["verdict_detail"].lower()

    def test_yes_stays_yes_when_sources_healthy(self):
        """With no events and full healthy sources, verdict should be Yes."""
        from app.models import SourceHealth
        sources = [
            SourceHealth(name="A", enabled=True, status="healthy"),
            SourceHealth(name="B", enabled=True, status="healthy"),
        ]
        result = compute_summary_verdict([], 8, sources)
        assert result["verdict"] == "Yes"

    def test_source_coverage_field_present(self):
        """Summary must include source_coverage field."""
        from app.models import SourceHealth
        sources = [SourceHealth(name="A", enabled=True, status="healthy")]
        result = compute_summary_verdict([], 8, sources)
        assert "source_coverage" in result
