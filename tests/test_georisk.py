import pytest

from app.georisk import GeopoliticalRiskService, MONITORS


def _article(title: str, seendate: str = "20260317T090000Z", source_family: str = "google_news", kind: str = "escalation") -> dict:
    return {
        "title": title,
        "url": "https://example.com/story",
        "domain": "example.com",
        "seendate": seendate,
        "source_family": source_family,
        "kind": kind,
    }


@pytest.fixture(autouse=True)
def _silence_aux_sources(monkeypatch):
    async def _empty(*args, **kwargs):
        return []

    monkeypatch.setattr(GeopoliticalRiskService, "_query_google_news", _empty)
    monkeypatch.setattr(GeopoliticalRiskService, "_query_newsapi", _empty)
    monkeypatch.setattr(GeopoliticalRiskService, "_query_treasury_press", _empty)
    monkeypatch.setattr(GeopoliticalRiskService, "_query_defense_releases", _empty)
    monkeypatch.setattr(GeopoliticalRiskService, "_query_state_press", _empty)
    monkeypatch.setattr(GeopoliticalRiskService, "_query_ofac_recent_actions", _empty)


@pytest.mark.asyncio
async def test_diplomacy_heavy_flashpoint_does_not_drop_to_zero(monkeypatch):
    svc = GeopoliticalRiskService()

    async def fake_google(query: str, hours: int, source_family: str, kind: str):
        if kind == "deescalation":
            return [_article(f"Diplomatic update {i}", source_family="google_news", kind="deescalation") for i in range(40)]
        return []

    monkeypatch.setattr(svc, "_query_google_news", fake_google)

    score = await svc._assess(MONITORS[0])
    assert score.score > 0
    assert score.score < 25
    assert score.components["background_context"] > 0
    assert "background geopolitical tension" in score.detail.lower()
    assert score.coverage_articles == 40


@pytest.mark.asyncio
async def test_zero_coverage_can_still_score_zero():
    svc = GeopoliticalRiskService()
    score = await svc._assess(MONITORS[0])
    assert score.score == 0
    assert score.coverage_articles == 0
    assert score.source_status == "live"


@pytest.mark.asyncio
async def test_headlines_fall_back_to_diplomacy_coverage(monkeypatch):
    svc = GeopoliticalRiskService()

    async def fake_state(*args, **kwargs):
        return [_article("Regional diplomats push ceasefire", source_family="state_press", kind="deescalation")]

    monkeypatch.setattr(svc, "_query_state_press", fake_state)

    score = await svc._assess(MONITORS[0])
    assert score.escalation_articles == 0
    assert score.deescalation_articles >= 1
    assert score.top_headlines
    assert score.top_headlines[0]["title"] == "Regional diplomats push ceasefire"


@pytest.mark.asyncio
async def test_partial_source_failure_returns_assessing_not_false_zero(monkeypatch):
    svc = GeopoliticalRiskService()

    async def broken(*args, **kwargs):
        raise RuntimeError("timeout")

    monkeypatch.setattr(svc, "_query_google_news", broken)
    monkeypatch.setattr(svc, "_query_treasury_press", broken)
    monkeypatch.setattr(svc, "_query_defense_releases", broken)
    monkeypatch.setattr(svc, "_query_state_press", broken)
    monkeypatch.setattr(svc, "_query_ofac_recent_actions", broken)

    scores = await svc.get_scores(force=True)
    assert scores
    score = scores[0]
    assert score["source_status"] == "delayed"
    assert "partial source coverage" in score["detail"].lower() or "assessing" in score["detail"].lower()


@pytest.mark.asyncio
async def test_cache_is_used_within_ttl(monkeypatch):
    svc = GeopoliticalRiskService()

    async def fake_google(*args, **kwargs):
        return [_article("Missile strike reported", source_family="google_news")]

    monkeypatch.setattr(svc, "_query_google_news", fake_google)
    first = await svc.get_scores(force=True)
    assert first[0]["source_status"] == "live"

    async def broken(*args, **kwargs):
        raise AssertionError("should not be called while cache is fresh")

    monkeypatch.setattr(svc, "_query_google_news", broken)
    second = await svc.get_scores(force=False)
    assert second[0]["source_status"] == "live"
    assert second[0]["score"] == first[0]["score"]


@pytest.mark.asyncio
async def test_recent_risk_factors_are_extracted_explicitly(monkeypatch):
    svc = GeopoliticalRiskService()

    async def fake_google(query: str, hours: int, source_family: str, kind: str):
        if kind == "deescalation":
            return [_article("Regional diplomats push ceasefire in Gaza", "20260317T084500Z", "google_news", "deescalation")]
        return [
            _article("Missile strike raises fears over Strait of Hormuz shipping", "20260317T090000Z", "google_news"),
            _article("Pentagon deployment follows retaliation warning", "20260317T083000Z", "google_news"),
        ]

    monkeypatch.setattr(svc, "_query_google_news", fake_google)
    score = await svc._assess(MONITORS[0])
    labels = {f["label"] for f in score.risk_factors}
    assert "Missile / airstrike activity" in labels
    assert "Retaliation cycle" in labels or "US military posture" in labels
    assert "Diplomatic activity" in labels


@pytest.mark.asyncio
async def test_factor_diversity_and_source_breadth_components_are_populated(monkeypatch):
    svc = GeopoliticalRiskService()

    async def fake_google(*args, **kwargs):
        return [
            {**_article("Missile strike reported", "20260317T090000Z", "google_news"), "domain": "a.example"},
            {**_article("Oil refinery disruption feared", "20260317T083000Z", "google_news"), "domain": "b.example"},
        ]

    async def fake_state(*args, **kwargs):
        return [{**_article("State condemns shipping disruption", "20260317T081500Z", "state_press"), "domain": "state.gov"}]

    monkeypatch.setattr(svc, "_query_google_news", fake_google)
    monkeypatch.setattr(svc, "_query_state_press", fake_state)
    score = await svc._assess(MONITORS[0])
    assert score.components["factor_diversity"] > 0
    assert score.components["source_breadth"] > 0
    assert score.components["risk_factor_count"] >= 2


@pytest.mark.asyncio
async def test_freshness_metadata_is_exposed_on_live_scores(monkeypatch):
    svc = GeopoliticalRiskService()

    async def fake_google(*args, **kwargs):
        return [_article("Missile strike reported", source_family="google_news")]

    monkeypatch.setattr(svc, "_query_google_news", fake_google)
    scores = await svc.get_scores(force=True)
    score = scores[0]
    assert score["updated_utc"]
    assert score["last_live_utc"]
    assert score["next_live_reassess_utc"]
    assert score["signal_sources"]


@pytest.mark.asyncio
async def test_official_sources_can_support_scoring_without_gdelt(monkeypatch):
    svc = GeopoliticalRiskService()

    async def fake_treasury(*args, **kwargs):
        return [_article("Treasury sanctions tanker network linked to Iran", source_family="treasury_press")]

    async def fake_state(*args, **kwargs):
        return [_article("State says humanitarian talks continue in Gaza", source_family="state_press", kind="deescalation")]

    monkeypatch.setattr(svc, "_query_treasury_press", fake_treasury)
    monkeypatch.setattr(svc, "_query_state_press", fake_state)
    scores = await svc.get_scores(force=True)
    score = scores[0]
    assert score["source_status"] == "live"
    assert score["coverage_articles"] >= 2
    names = {s["name"] for s in score["signal_sources"]}
    assert "Treasury press" in names
    assert "State press" in names


def test_extract_risk_factors_carries_source_counts():
    svc = GeopoliticalRiskService()
    factors = svc._extract_risk_factors(
        [
            {**_article("Missile strike reported", source_family="google_news")},
            {**_article("Missile strike reported near shipping lane", source_family="state_press")},
        ],
        [],
    )
    assert factors
    assert factors[0]["source_count"] >= 1



def test_duplicate_article_does_not_appear_across_multiple_risk_tiles():
    svc = GeopoliticalRiskService()
    shared = {**_article("Israeli Strike in Gaza Kills Three as Iran War Strains Ceasefire", "20260317T090000Z", "state_press")}
    factors = svc._extract_risk_factors([shared], [shared])
    titles = [f.get("latest_title") for f in factors]
    assert titles.count("Israeli Strike in Gaza Kills Three as Iran War Strains Ceasefire") == 1
    missile = next(f for f in factors if f["label"] == "Missile / airstrike activity")
    assert missile["latest_title"] == "Israeli Strike in Gaza Kills Three as Iran War Strains Ceasefire"



def test_mixed_conflict_and_ceasefire_headline_prefers_conflict_factor():
    svc = GeopoliticalRiskService()
    factors = svc._extract_risk_factors(
        [{**_article("Israeli Strike in Gaza Kills Three as Iran War Strains Ceasefire", "20260317T090000Z", "google_news")}],
        [],
    )
    assert factors[0]["label"] == "Missile / airstrike activity"


def test_middle_east_region_gate_excludes_nicaragua_sanctions_title():
    svc = GeopoliticalRiskService()
    monitor = MONITORS[0]
    assert not svc._matches_monitor_region(
        "treasury sanctions nicaraguan officials enabling the murillo-ortega dictatorship repression",
        monitor,
    )


def test_middle_east_region_gate_keeps_iran_sanctions_title():
    svc = GeopoliticalRiskService()
    monitor = MONITORS[0]
    assert svc._matches_monitor_region(
        "treasury sanctions shipping network moving iranian oil",
        monitor,
    )
