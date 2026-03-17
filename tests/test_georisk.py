import pytest

from app.georisk import GeopoliticalRiskService, MONITORS


def _article(title: str, seendate: str = "20260317T090000Z") -> dict:
    return {
        "title": title,
        "url": "https://example.com/story",
        "domain": "example.com",
        "seendate": seendate,
    }


@pytest.fixture(autouse=True)
def _silence_aux_sources(monkeypatch):
    async def _empty(*args, **kwargs):
        return []

    monkeypatch.setattr(GeopoliticalRiskService, "_query_google_news", _empty)
    monkeypatch.setattr(GeopoliticalRiskService, "_query_newsapi", _empty)
    monkeypatch.setattr(GeopoliticalRiskService, "_query_treasury_press", _empty)
    monkeypatch.setattr(GeopoliticalRiskService, "_query_defense_releases", _empty)

@pytest.mark.asyncio
async def test_diplomacy_heavy_flashpoint_does_not_drop_to_zero(monkeypatch):
    svc = GeopoliticalRiskService()

    async def fake_query(query: str, hours: int):
        if "ceasefire" in query or "diplomatic" in query or "humanitarian" in query:
            return [_article(f"Diplomatic update {i}") for i in range(40)]
        return []

    monkeypatch.setattr(svc, "_query_gdelt", fake_query)

    score = await svc._assess(MONITORS[0])
    assert score.score > 0
    assert score.score < 25
    assert score.components["background_context"] > 0
    assert "background geopolitical tension" in score.detail.lower()
    assert score.coverage_articles == 40


@pytest.mark.asyncio
async def test_zero_coverage_can_still_score_zero(monkeypatch):
    svc = GeopoliticalRiskService()

    async def fake_query(query: str, hours: int):
        return []

    monkeypatch.setattr(svc, "_query_gdelt", fake_query)

    score = await svc._assess(MONITORS[0])
    assert score.score == 0
    assert score.coverage_articles == 0


@pytest.mark.asyncio
async def test_headlines_fall_back_to_diplomacy_coverage(monkeypatch):
    svc = GeopoliticalRiskService()

    async def fake_query(query: str, hours: int):
        if "ceasefire" in query or "diplomatic" in query or "humanitarian" in query:
            return [_article("Regional diplomats push ceasefire") for _ in range(3)]
        return []

    monkeypatch.setattr(svc, "_query_gdelt", fake_query)

    score = await svc._assess(MONITORS[0])
    assert score.escalation_articles == 0
    assert score.deescalation_articles >= 1
    assert score.top_headlines
    assert score.top_headlines[0]["title"] == "Regional diplomats push ceasefire"


@pytest.mark.asyncio
async def test_rate_limited_georisk_returns_delayed_not_false_zero(monkeypatch):
    svc = GeopoliticalRiskService()

    async def fake_query(query: str, hours: int):
        from app.georisk import RateLimitError
        raise RateLimitError(600)

    monkeypatch.setattr(svc, "_query_gdelt", fake_query)

    scores = await svc.get_scores(force=True)
    assert scores
    score = scores[0]
    assert score["source_status"] == "delayed"
    assert "rate-limited" in score["detail"].lower()


@pytest.mark.asyncio
async def test_stale_cache_is_used_during_cooldown(monkeypatch):
    svc = GeopoliticalRiskService()

    async def first_query(query: str, hours: int):
        return [_article("Conflict update")]

    monkeypatch.setattr(svc, "_query_gdelt", first_query)
    first = await svc.get_scores(force=True)
    assert first[0]["source_status"] == "live"

    async def rate_limited(query: str, hours: int):
        from app.georisk import RateLimitError
        raise RateLimitError(600)

    monkeypatch.setattr(svc, "_query_gdelt", rate_limited)
    second = await svc.get_scores(force=True)
    assert second[0]["source_status"] == "stale"
    assert second[0]["score"] == first[0]["score"]


@pytest.mark.asyncio
async def test_recent_risk_factors_are_extracted_explicitly(monkeypatch):
    svc = GeopoliticalRiskService()

    async def fake_query(query: str, hours: int):
        if "ceasefire" in query or "diplomatic" in query or "humanitarian" in query:
            return [
                _article("Regional diplomats push ceasefire in Gaza", "20260317T084500Z"),
            ]
        return [
            _article("Missile strike raises fears over Strait of Hormuz shipping", "20260317T090000Z"),
            _article("Pentagon deployment follows retaliation warning", "20260317T083000Z"),
        ]

    monkeypatch.setattr(svc, "_query_gdelt", fake_query)
    score = await svc._assess(MONITORS[0])
    labels = {f["label"] for f in score.risk_factors}
    assert "Missile / airstrike activity" in labels
    assert "Retaliation cycle" in labels or "US military posture" in labels
    assert "Diplomatic activity" in labels


@pytest.mark.asyncio
async def test_factor_diversity_and_source_breadth_components_are_populated(monkeypatch):
    svc = GeopoliticalRiskService()

    async def fake_query(query: str, hours: int):
        if "ceasefire" in query or "diplomatic" in query or "humanitarian" in query:
            return []
        return [
            {**_article("Missile strike reported", "20260317T090000Z"), "domain": "a.example"},
            {**_article("Oil refinery disruption feared", "20260317T083000Z"), "domain": "b.example"},
            {**_article("Pentagon deployment follows retaliation", "20260317T081500Z"), "domain": "c.example"},
        ]

    monkeypatch.setattr(svc, "_query_gdelt", fake_query)
    score = await svc._assess(MONITORS[0])
    assert score.components["factor_diversity"] > 0
    assert score.components["source_breadth"] > 0
    assert score.components["risk_factor_count"] >= 3


@pytest.mark.asyncio
async def test_freshness_metadata_is_exposed_on_live_scores(monkeypatch):
    svc = GeopoliticalRiskService()

    async def fake_query(query: str, hours: int):
        return [_article("Missile strike reported") ]

    monkeypatch.setattr(svc, "_query_gdelt", fake_query)
    scores = await svc.get_scores(force=True)
    score = scores[0]
    assert score["updated_utc"]
    assert score["last_live_utc"]
    assert score["next_live_reassess_utc"]
    assert score["signal_sources"]


@pytest.mark.asyncio
async def test_auxiliary_sources_can_support_assessing_mode(monkeypatch):
    svc = GeopoliticalRiskService()

    async def rate_limited(query: str, hours: int):
        from app.georisk import RateLimitError
        raise RateLimitError(600)

    async def fake_google(*args, **kwargs):
        return [_article("Treasury sanctions tanker network linked to Iran", "20260317T090000Z")]

    monkeypatch.setattr(svc, "_query_gdelt", rate_limited)
    monkeypatch.setattr(svc, "_query_google_news", fake_google)
    scores = await svc.get_scores(force=True)
    score = scores[0]
    assert score["source_status"] == "delayed"
    assert score["coverage_articles"] >= 1
    assert score["signal_sources"]


def test_extract_risk_factors_carries_source_counts():
    svc = GeopoliticalRiskService()
    factors = svc._extract_risk_factors(
        [
            {**_article("Missile strike reported"), "source_family": "gdelt"},
            {**_article("Missile strike reported near shipping lane"), "source_family": "google_news"},
        ],
        [],
    )
    assert factors
    assert factors[0]["source_count"] >= 1


def test_duplicate_article_does_not_appear_across_multiple_risk_tiles():
    svc = GeopoliticalRiskService()
    shared = {**_article("Israeli Strike in Gaza Kills Three as Iran War Strains Ceasefire", "20260317T090000Z"), "source_family": "gdelt"}
    factors = svc._extract_risk_factors([shared], [shared])
    titles = [f.get("latest_title") for f in factors]
    assert titles.count("Israeli Strike in Gaza Kills Three as Iran War Strains Ceasefire") == 1
    missile = next(f for f in factors if f["label"] == "Missile / airstrike activity")
    assert missile["latest_title"] == "Israeli Strike in Gaza Kills Three as Iran War Strains Ceasefire"


def test_mixed_conflict_and_ceasefire_headline_prefers_conflict_factor():
    svc = GeopoliticalRiskService()
    factors = svc._extract_risk_factors(
        [{**_article("Israeli Strike in Gaza Kills Three as Iran War Strains Ceasefire", "20260317T090000Z"), "source_family": "gdelt"}],
        [],
    )
    assert factors[0]["label"] == "Missile / airstrike activity"
