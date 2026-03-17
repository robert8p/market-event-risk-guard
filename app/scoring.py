"""
Rule-based materiality scoring engine.

Determines whether an event has realistic potential to cause abnormal
volatility, directional repricing, liquidity withdrawal, spread widening,
correlation spikes, or technical-signal unreliability.

Framework
---------
1. Base event weight by event type
2. Breadth-of-impact adjustment
3. Surprise / uncertainty adjustment
4. Time-proximity adjustment
5. Breaking-news premium
6. Regime-shift premium
7. Confidence score

Threshold mapping
-----------------
  0–34  → Low      → Technicals usable
 35–54  → Moderate → Use caution
 55–74  → High     → Avoid new entries near event
 75–100 → Extreme  → Stand aside until event passes
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from app.models import (
    AssetClass,
    CautionLevel,
    Classification,
    EventCategory,
    NormalisedEvent,
    SuggestedAction,
)


# ── Base weights by event type keyword ───────────────────────────────────────

BASE_WEIGHTS: dict[str, int] = {
    # Macro – top tier
    "rate_decision": 72,
    "fomc": 72,
    "fed_rate": 72,
    "ecb_rate": 68,
    "boe_rate": 65,
    "cpi": 65,
    "nonfarm_payrolls": 65,
    "nfp": 65,
    # Macro – high
    "ppi": 52,
    "gdp": 55,
    "pmi": 48,
    "ism": 48,
    "retail_sales": 45,
    "unemployment": 50,
    "consumer_confidence": 40,
    "central_bank_minutes": 55,
    "central_bank_speech": 42,
    "treasury_auction": 35,
    "policy_announcement": 55,
    "sanctions": 60,
    "tariffs": 58,
    "geopolitical": 62,
    "government_shutdown": 55,
    # Equity
    "earnings": 45,
    "mega_cap_earnings": 60,
    "guidance": 48,
    "index_rebalance": 38,
    "options_expiry": 42,
    "sec_decision": 45,
    "sec_action": 50,
    "sec_routine": 18,
    "mna": 45,
    "trading_halt": 55,
    # Crypto
    "token_unlock": 42,
    "etf_decision": 62,
    "crypto_regulatory": 55,
    "exchange_outage": 52,
    "exchange_maintenance": 30,
    "listing": 35,
    "delisting": 42,
    "hard_fork": 50,
    "protocol_upgrade": 45,
    "stablecoin_depeg": 68,
    "bankruptcy": 58,
    "governance": 32,
    "binance_routine": 12,
    "crypto_event": 20,
    # Cross-asset / systemic
    "war": 78,
    "conflict": 72,
    "emergency_cb_action": 80,
    "legal_ruling": 48,
    "infrastructure_incident": 55,
    "policy_intervention": 60,
}

DEFAULT_BASE_WEIGHT = 30


# ── Affected-instruments lookup ──────────────────────────────────────────────

INSTRUMENTS_BY_ASSET: dict[AssetClass, list[str]] = {
    AssetClass.EQUITIES: ["ES", "NQ", "SPY", "QQQ", "DIA", "mega-cap tech"],
    AssetClass.CRYPTO: ["BTC-USD", "ETH-USD", "SOL-USD"],
    AssetClass.BOTH: [
        "ES", "NQ", "SPY", "QQQ", "BTC-USD", "ETH-USD",
        "dollar-sensitive assets", "rates-sensitive assets",
    ],
}

MACRO_INSTRUMENTS = [
    "ES", "NQ", "SPY", "QQQ", "TLT", "DXY",
    "BTC-USD", "ETH-USD", "rates-sensitive assets",
]


# ── Why-it-matters templates ─────────────────────────────────────────────────

WHY_TEMPLATES: dict[str, str] = {
    "rate_decision": "Central bank rate decisions directly reprice risk assets, move yields, and can trigger volatility spikes across equities, crypto, and FX.",
    "cpi": "CPI prints drive rate-cut expectations. A surprise reading can reprice the entire yield curve and trigger sharp equity and crypto moves.",
    "nonfarm_payrolls": "Payroll data shapes Fed policy expectations. A miss or beat can cause rapid repricing in equities, bonds, and risk assets.",
    "ppi": "PPI data influences inflation expectations and can move rate-sensitive assets.",
    "gdp": "GDP releases affect growth outlook and risk appetite across asset classes.",
    "pmi": "PMI data signals economic momentum. Surprise readings move equities and rate expectations.",
    "earnings": "Major earnings announcements can move individual stocks, sectors, and index futures—especially mega-cap names.",
    "mega_cap_earnings": "Mega-cap earnings carry outsized index weight. A miss or guidance change can drag index futures and spike volatility.",
    "etf_decision": "Crypto ETF decisions directly affect institutional flow expectations and can trigger sharp moves in BTC and ETH.",
    "exchange_outage": "Exchange outages reduce liquidity, widen spreads, and can trigger cascading liquidations across crypto markets.",
    "hard_fork": "Hard forks and protocol upgrades create uncertainty around chain continuity and can cause price dislocations.",
    "stablecoin_depeg": "Stablecoin depeg events threaten DeFi collateral and can trigger broad crypto contagion.",
    "sanctions": "Sanctions announcements can disrupt trade flows, spike commodity prices, and trigger risk-off moves.",
    "tariffs": "Tariff escalation raises input costs, disrupts supply chains, and can trigger broad equity selling.",
    "geopolitical": "Geopolitical escalation introduces tail risk and can trigger flight-to-safety moves across all risk assets.",
    "war": "Armed conflict creates extreme uncertainty, commodity spikes, and potential liquidity shocks.",
    "fomc": "FOMC meetings are the single most important scheduled macro event for rates, equities, and crypto.",
    "ecb_rate": "ECB rate decisions move European yields, EUR/USD, and can spill over into US equity and crypto markets.",
    "treasury_auction": "Treasury auctions reveal demand for government debt. Weak auctions can spike yields and pressure risk assets.",
    "listing": "Major exchange listings create sudden demand and volatility for affected tokens and can spill into correlated pairs.",
    "delisting": "Exchange delistings can trigger forced selling, liquidity withdrawal, and contagion to related tokens.",
    "exchange_maintenance": "Scheduled exchange maintenance reduces available liquidity and can cause temporary price dislocations.",
}

DEFAULT_WHY = "This event has the potential to distort normal price behaviour and make technical signals unreliable."


# ── Public API ───────────────────────────────────────────────────────────────

def score_event(event: NormalisedEvent) -> NormalisedEvent:
    """Apply the full scoring framework and mutate the event in place."""

    # ── Step 1: Derive confidence FIRST so the discount uses the real value ──
    if event.confidence == 0.5:
        event.confidence = _derive_confidence(event)

    base = _base_weight(event.event_type)
    breadth = _breadth_adjustment(event.asset_class, event.category)
    surprise = _surprise_adjustment(event.classification)
    proximity = _time_proximity_adjustment(event.start_time_utc)
    breaking = _breaking_premium(event.classification)
    regime = _regime_shift_premium(event.event_type)

    raw = base + breadth + surprise + proximity + breaking + regime

    # ── Confidence discount ──────────────────────────────────────────────
    # Uses the ACTUAL confidence (derived above), not the placeholder 0.5.
    # High-confidence scheduled events keep full score.
    # Low-confidence individual news articles are discounted.
    conf = event.confidence
    if conf < 0.8:
        discount = 0.45 + (conf * 0.5)
        raw = int(raw * discount)

    score = max(0, min(100, raw))

    event.impact_score = score
    event.caution_level = _caution_level(score)
    event.suggested_action = _suggested_action(score)

    if not event.why_it_matters:
        event.why_it_matters = _why_it_matters(event.event_type)

    if not event.affected_instruments:
        event.affected_instruments = _affected_instruments(event)

    event.caution_window_start_utc, event.caution_window_end_utc = _caution_window(
        event.start_time_utc, event.end_time_utc, score
    )

    now = datetime.now(timezone.utc)
    diff = (event.start_time_utc - now).total_seconds() / 60.0
    event.countdown_minutes = round(max(diff, 0), 1)

    return event


def compute_summary_verdict(
    events: list[NormalisedEvent],
    window_hours: int = 8,
    sources: list | None = None,
) -> dict:
    """Produce a summary verdict for the given event window.

    When source coverage is degraded, the verdict is softened — a "Yes" becomes
    "Caution" because the absence of detected events might mean sources are down,
    not that there is genuinely no risk.
    """
    from app.models import EnvironmentLabel, RiskSummary, SourceHealth

    # ── Assess source coverage ───────────────────────────────────────────
    source_coverage = "full"
    if sources:
        enabled = [s for s in sources if s.enabled]
        healthy_count = sum(1 for s in enabled if s.status == "healthy")
        pending_count = sum(1 for s in enabled if s.status == "pending")
        total = len(enabled)

        if total == 0 or healthy_count == 0:
            source_coverage = "none"
        elif pending_count > 0:
            source_coverage = "partial"
        elif healthy_count < total * 0.5:
            source_coverage = "degraded"
        elif healthy_count < total:
            source_coverage = "partial"
        else:
            source_coverage = "full"

    # Events are already filtered by the service layer — use them directly
    window_events = list(events)

    if not window_events:
        if source_coverage in ("none", "degraded"):
            return RiskSummary(
                window_hours=window_hours,
                source_coverage=source_coverage,
                verdict="Caution",
                verdict_detail="No events detected, but source coverage is degraded. Cannot confirm safety.",
                environment_label=EnvironmentLabel.EVENT_SENSITIVE,
                highest_caution=CautionLevel.MODERATE,
            ).model_dump(mode="json")
        elif source_coverage == "partial":
            return RiskSummary(
                window_hours=window_hours,
                source_coverage=source_coverage,
                verdict="Caution",
                verdict_detail="No events detected, but some sources are still loading or unavailable. Verdict may change.",
            ).model_dump(mode="json")
        return RiskSummary(window_hours=window_hours, source_coverage=source_coverage).model_dump(mode="json")

    highest = max(window_events, key=lambda e: e.impact_score)
    # Separate ongoing from upcoming for banner logic
    ongoing = [e for e in window_events if e.classification == Classification.ONGOING]
    # Imminent = upcoming events within 60 min (not ongoing — those are already active)
    imminent = [
        e for e in window_events
        if e.classification != Classification.ONGOING
        and e.countdown_minutes is not None
        and e.countdown_minutes <= 60
        and e.impact_score >= 35
    ]
    top_distortion = sorted(window_events, key=lambda e: e.impact_score, reverse=True)[:5]

    caution_order = [CautionLevel.LOW, CautionLevel.MODERATE, CautionLevel.HIGH, CautionLevel.EXTREME]
    highest_caution = max((e.caution_level for e in window_events), key=lambda c: caution_order.index(c))

    # Determine environment label
    if highest_caution == CautionLevel.EXTREME:
        env_label = EnvironmentLabel.HIGH_RISK
    elif highest_caution in (CautionLevel.HIGH, CautionLevel.MODERATE):
        env_label = EnvironmentLabel.EVENT_SENSITIVE
    else:
        env_label = EnvironmentLabel.NORMAL

    # Determine verdict
    extreme_count = sum(1 for e in window_events if e.caution_level == CautionLevel.EXTREME)
    high_count = sum(1 for e in window_events if e.caution_level == CautionLevel.HIGH)
    moderate_count = sum(1 for e in window_events if e.caution_level == CautionLevel.MODERATE)

    if extreme_count > 0:
        verdict = "No"
        verdict_detail = f"{extreme_count} extreme-risk event(s) detected. Technicals may be unreliable."
    elif high_count >= 2 or (high_count >= 1 and moderate_count >= 2):
        verdict = "No"
        verdict_detail = f"Cluster of {high_count} high and {moderate_count} moderate event(s). Technical signals at elevated risk."
    elif high_count == 1:
        verdict = "Caution"
        verdict_detail = "One high-impact event approaching. Use reduced position sizing and wider stops."
    elif moderate_count >= 1:
        verdict = "Caution"
        verdict_detail = f"{moderate_count} moderate-risk event(s) in window. Be aware of scheduled volatility."
    else:
        verdict = "Yes"
        verdict_detail = "No major event risk detected. Technicals usable with normal confidence."

    # ── Coverage-aware softening ─────────────────────────────────────────
    # A "Yes" verdict when source coverage is degraded is unreliable —
    # the absence of high-risk events might just mean we couldn't see them.
    if verdict == "Yes" and source_coverage in ("degraded", "none"):
        verdict = "Caution"
        verdict_detail = (
            "No event risk detected in available sources, but coverage is degraded. "
            "Some sources are down or unconfigured — cannot confirm full safety."
        )
        env_label = EnvironmentLabel.EVENT_SENSITIVE
    elif verdict == "Yes" and source_coverage == "partial":
        verdict_detail += " Note: some sources still loading or unavailable."

    # Banner alert
    banner = None
    # Priority 1: ongoing high-impact incidents
    high_ongoing = [e for e in ongoing if e.impact_score >= 55]
    if high_ongoing:
        e = high_ongoing[0]
        banner = f"⚠ ONGOING: {e.title} — active incident, avoid affected instruments"
    # Priority 2: extreme imminent events
    extreme_imminent = [e for e in imminent if e.caution_level == CautionLevel.EXTREME]
    if not banner and extreme_imminent:
        banner = f"⚠ EXTREME: {extreme_imminent[0].title} in {int(extreme_imminent[0].countdown_minutes or 0)} min — stand aside until event passes"
    elif not banner and len(imminent) >= 3:
        banner = f"⚠ CLUSTER: {len(imminent)} events within 60 minutes — elevated distortion risk"
    elif not banner and imminent:
        banner = f"⚠ IMMINENT: {imminent[0].title} in {int(imminent[0].countdown_minutes or 0)} min — review open positions"

    summary = RiskSummary(
        event_count=len(window_events),
        highest_caution=highest_caution,
        environment_label=env_label,
        verdict=verdict,
        verdict_detail=verdict_detail,
        imminent_events=imminent,
        ongoing_events=ongoing,
        highest_distortion_events=top_distortion,
        banner_alert=banner,
        window_hours=window_hours,
        source_coverage=source_coverage,
    )
    return summary.model_dump(mode="json")


# ── Internal helpers ─────────────────────────────────────────────────────────

def _base_weight(event_type: str) -> int:
    et = event_type.lower().replace(" ", "_").replace("-", "_")
    for key, val in BASE_WEIGHTS.items():
        if key in et:
            return val
    return DEFAULT_BASE_WEIGHT


def _breadth_adjustment(asset_class: AssetClass, category: EventCategory) -> int:
    if asset_class == AssetClass.BOTH:
        return 8
    if category in (EventCategory.CROSS_ASSET, EventCategory.SYSTEMIC):
        return 10
    return 0


def _surprise_adjustment(classification: Classification) -> int:
    if classification == Classification.BREAKING:
        return 10
    if classification == Classification.ONGOING:
        return 4
    return 0


def _time_proximity_adjustment(start_utc: datetime) -> int:
    now = datetime.now(timezone.utc)
    minutes_away = (start_utc - now).total_seconds() / 60.0
    if minutes_away <= 15:
        return 12
    if minutes_away <= 30:
        return 8
    if minutes_away <= 60:
        return 5
    if minutes_away <= 120:
        return 2
    return 0


def _breaking_premium(classification: Classification) -> int:
    return 6 if classification == Classification.BREAKING else 0


def _regime_shift_premium(event_type: str) -> int:
    regime_keywords = [
        "war", "conflict", "emergency", "stablecoin_depeg",
        "sanctions", "tariffs", "geopolitical", "shutdown",
    ]
    et = event_type.lower()
    for kw in regime_keywords:
        if kw in et:
            return 8
    return 0


def _caution_level(score: int) -> CautionLevel:
    if score >= 75:
        return CautionLevel.EXTREME
    if score >= 55:
        return CautionLevel.HIGH
    if score >= 35:
        return CautionLevel.MODERATE
    return CautionLevel.LOW


def _suggested_action(score: int) -> SuggestedAction:
    if score >= 75:
        return SuggestedAction.STAND_ASIDE
    if score >= 55:
        return SuggestedAction.AVOID_NEW_ENTRIES
    if score >= 35:
        return SuggestedAction.USE_CAUTION
    return SuggestedAction.TECHNICALS_USABLE


def _why_it_matters(event_type: str) -> str:
    et = event_type.lower()
    for key, text in WHY_TEMPLATES.items():
        if key in et:
            return text
    return DEFAULT_WHY


def _affected_instruments(event: NormalisedEvent) -> list[str]:
    if event.category == EventCategory.MACRO or event.asset_class == AssetClass.BOTH:
        return MACRO_INSTRUMENTS.copy()
    return INSTRUMENTS_BY_ASSET.get(event.asset_class, ["broad risk assets"]).copy()


def _caution_window(
    start_utc: datetime,
    end_utc: Optional[datetime],
    score: int,
) -> tuple[datetime, datetime]:
    if score >= 75:
        lead = timedelta(minutes=45)
        trail = timedelta(minutes=60)
    elif score >= 55:
        lead = timedelta(minutes=30)
        trail = timedelta(minutes=45)
    elif score >= 35:
        lead = timedelta(minutes=15)
        trail = timedelta(minutes=30)
    else:
        lead = timedelta(minutes=5)
        trail = timedelta(minutes=15)

    window_start = start_utc - lead
    window_end = (end_utc or start_utc) + trail
    return window_start, window_end


def _derive_confidence(event: NormalisedEvent) -> float:
    score = 0.5
    if event.source_name:
        score += 0.1
    if event.source_url:
        score += 0.05
    if event.classification == Classification.SCHEDULED:
        score += 0.15
    elif event.classification == Classification.BREAKING:
        score -= 0.1
    if event.description and len(event.description) > 20:
        score += 0.1
    return round(max(0.1, min(1.0, score)), 2)
