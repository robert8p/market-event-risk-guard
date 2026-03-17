"""Tests for the scoring engine."""
import pytest
from datetime import datetime, timedelta, timezone
from app.models import AssetClass, CautionLevel, Classification, EventCategory, NormalisedEvent, SuggestedAction
from app.scoring import score_event, compute_summary_verdict

def _make_event(**overrides):
    defaults = dict(id="test-001",title="Test Event",category=EventCategory.MACRO,event_type="cpi",classification=Classification.SCHEDULED,asset_class=AssetClass.BOTH,start_time_utc=datetime.now(timezone.utc)+timedelta(minutes=30),impact_score=0,caution_level=CautionLevel.LOW,suggested_action=SuggestedAction.TECHNICALS_USABLE,confidence=0.5,source_name="TestSource")
    defaults.update(overrides)
    return NormalisedEvent(**defaults)

class TestScoring:
    def test_cpi_scores_high(self):
        ev = _make_event(event_type="cpi")
        scored = score_event(ev)
        assert scored.impact_score >= 55
        assert scored.caution_level in (CautionLevel.HIGH, CautionLevel.EXTREME)

    def test_fomc_scores_extreme_when_imminent(self):
        ev = _make_event(event_type="fomc", start_time_utc=datetime.now(timezone.utc)+timedelta(minutes=10))
        scored = score_event(ev)
        assert scored.impact_score >= 75
        assert scored.caution_level == CautionLevel.EXTREME

    def test_minor_scores_low(self):
        ev = _make_event(event_type="minor_data", start_time_utc=datetime.now(timezone.utc)+timedelta(hours=6))
        scored = score_event(ev)
        assert scored.impact_score < 55

    def test_breaking_premium(self):
        s1 = score_event(_make_event(event_type="economic", classification=Classification.SCHEDULED))
        s2 = score_event(_make_event(event_type="economic", classification=Classification.BREAKING))
        assert s2.impact_score > s1.impact_score

    def test_countdown(self):
        ev = _make_event(start_time_utc=datetime.now(timezone.utc)+timedelta(minutes=45))
        scored = score_event(ev)
        assert 40 <= scored.countdown_minutes <= 50

    def test_caution_window(self):
        scored = score_event(_make_event(event_type="cpi"))
        assert scored.caution_window_start_utc < scored.start_time_utc

    def test_instruments(self):
        scored = score_event(_make_event(event_type="cpi", asset_class=AssetClass.BOTH))
        assert len(scored.affected_instruments) > 0

    def test_why_it_matters(self):
        scored = score_event(_make_event(event_type="cpi"))
        assert len(scored.why_it_matters) > 10

    def test_clamp(self):
        ev = _make_event(event_type="war", classification=Classification.BREAKING, asset_class=AssetClass.BOTH, start_time_utc=datetime.now(timezone.utc)+timedelta(minutes=5))
        scored = score_event(ev)
        assert 0 <= scored.impact_score <= 100

class TestSummaryVerdict:
    def test_empty_yields_yes(self):
        assert compute_summary_verdict([])["verdict"] == "Yes"

    def test_extreme_yields_no(self):
        ev = score_event(_make_event(event_type="fomc", start_time_utc=datetime.now(timezone.utc)+timedelta(minutes=10)))
        assert compute_summary_verdict([ev])["verdict"] == "No"

    def test_banner_extreme(self):
        ev = score_event(_make_event(event_type="fomc", start_time_utc=datetime.now(timezone.utc)+timedelta(minutes=10)))
        result = compute_summary_verdict([ev])
        assert result["banner_alert"] is not None
        assert "EXTREME" in result["banner_alert"]
