"""
Geopolitical threat monitor — computes escalation probability scores from
low-latency official sources plus broad headline coverage, then explains the score.

GDELT has been intentionally removed from score-setting because it introduced
rate-limit bottlenecks and false oscillation in the geopolitical card.
"""

from __future__ import annotations

import asyncio
import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from html import unescape
from typing import Optional
from urllib.parse import quote_plus, urljoin, urlparse

import httpx

from app.config import get_settings

logger = logging.getLogger(__name__)

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"
TREASURY_PRESS_URL = "https://home.treasury.gov/news/press-releases"
DEFENSE_RELEASES_RSS = "https://www.war.gov/DesktopModules/ArticleCS/RSS.ashx?ContentType=9&Site=945&max=10"
STATE_PRESS_URL = "https://www.state.gov/press-releases"
OFAC_RECENT_ACTIONS_URL = "https://ofac.treasury.gov/recent-actions"

LIVE_SCORE_TTL_SECONDS = 900


@dataclass
class ThreatMonitor:
    id: str
    label: str
    description: str
    escalation_query: str
    deescalation_query: str
    key_actors: list[str] = field(default_factory=list)
    severity_keywords: list[str] = field(default_factory=list)
    instruments: list[str] = field(default_factory=list)
    lookback_hours: int = 24


@dataclass
class ThreatScore:
    id: str
    label: str
    description: str
    score: int
    level: str
    detail: str
    escalation_articles: int
    deescalation_articles: int
    coverage_articles: int
    top_headlines: list[dict]
    risk_factors: list[dict]
    instruments: list[str]
    updated_utc: str
    components: dict
    source_status: str = "live"
    source_note: Optional[str] = None
    last_live_utc: Optional[str] = None
    next_live_reassess_utc: Optional[str] = None
    signal_sources: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "label": self.label,
            "description": self.description,
            "score": self.score,
            "level": self.level,
            "detail": self.detail,
            "escalation_articles": self.escalation_articles,
            "deescalation_articles": self.deescalation_articles,
            "coverage_articles": self.coverage_articles,
            "top_headlines": self.top_headlines,
            "risk_factors": self.risk_factors,
            "instruments": self.instruments,
            "updated_utc": self.updated_utc,
            "components": self.components,
            "source_status": self.source_status,
            "source_note": self.source_note,
            "last_live_utc": self.last_live_utc,
            "next_live_reassess_utc": self.next_live_reassess_utc,
            "signal_sources": self.signal_sources,
        }


MONITORS = [
    ThreatMonitor(
        id="middle_east",
        label="Middle East Escalation",
        description="Tracks military escalation risk across Iran, Israel, Yemen, Lebanon, and the Gulf states.",
        escalation_query="(Iran OR Israel OR Hezbollah OR Houthi OR Yemen OR Gaza) (attack OR strike OR missile OR retaliation OR casualties OR killed OR bombardment OR escalation)",
        deescalation_query="(Iran OR Israel OR Hezbollah OR Houthi OR Yemen OR Gaza) (ceasefire OR negotiations OR diplomatic OR truce OR agreement OR withdrawal OR humanitarian)",
        key_actors=["Iran", "Israel", "Hezbollah", "Houthi", "Yemen", "Gaza", "United States", "IDF", "IRGC", "Pentagon"],
        severity_keywords=[
            "nuclear",
            "ground invasion",
            "declaration of war",
            "all-out war",
            "mass casualties",
            "civilian deaths",
            "chemical weapon",
            "ballistic missile",
            "strait of hormuz",
            "blockade",
            "oil embargo",
            "emergency session",
            "retaliation",
            "carpet bombing",
            "aircraft carrier",
            "no-fly zone",
        ],
        instruments=["CL", "BZ", "GC", "ES", "NQ", "DXY", "BTC-USD", "VIX", "TLT"],
        lookback_hours=24,
    ),
]


FACTOR_SPECS = [
    {"label": "Missile / airstrike activity", "kind": "escalation", "patterns": ["missile", "airstrike", "air strike", "drone", "rocket", "bombardment", "shelling", "strike"]},
    {"label": "Retaliation cycle", "kind": "escalation", "patterns": ["retaliation", "counterstrike", "interception", "response strike", "reprisal"]},
    {"label": "Ground-force mobilisation", "kind": "escalation", "patterns": ["ground invasion", "troops", "mobilization", "mobilisation", "incursion", "offensive"]},
    {"label": "Proxy militia activity", "kind": "pressure", "patterns": ["hezbollah", "houthi", "militia", "proxy", "armed group"]},
    {"label": "Shipping / route disruption", "kind": "market", "patterns": ["strait of hormuz", "red sea", "shipping", "tanker", "vessel", "cargo ship", "maritime", "shipping lane"]},
    {"label": "Oil-supply disruption risk", "kind": "market", "patterns": ["oil", "refinery", "pipeline", "energy infrastructure", "production", "supply disruption", "shadow fleet"]},
    {"label": "Sanctions / export-control pressure", "kind": "market", "patterns": ["sanctions", "embargo", "export controls", "trade restrictions"]},
    {"label": "US military posture", "kind": "pressure", "patterns": ["pentagon", "aircraft carrier", "destroyer", "deployment", "u.s. forces", "us forces", "navy"]},
    {"label": "Nuclear programme tension", "kind": "escalation", "patterns": ["nuclear", "uranium", "enrichment", "atomic", "reactor"]},
    {"label": "Casualty / hostage risk", "kind": "escalation", "patterns": ["casualties", "killed", "wounded", "civilian", "hostage", "deaths"]},
    {"label": "Diplomatic activity", "kind": "deescalation", "patterns": ["ceasefire", "negotiations", "diplomatic", "truce", "agreement", "withdrawal", "humanitarian", "talks", "mediation", "envoy"]},
]

KIND_PRIORITY = {"escalation": 0, "market": 1, "pressure": 2, "deescalation": 3, "context": 4}
SOURCE_PRIORITY = {
    "defense_rss": 0,
    "state_press": 1,
    "treasury_press": 2,
    "ofac_recent": 3,
    "newsapi": 4,
    "google_news": 5,
    "unknown": 9,
}


class GeopoliticalRiskService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self._client: Optional[httpx.AsyncClient] = None
        self._cache: dict[str, ThreatScore] = {}
        self._cache_time: Optional[datetime] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.settings.http_timeout_seconds),
                follow_redirects=True,
                headers={"User-Agent": "MarketEventRiskGuard/2.3.1 (+https://render.com)"},
            )
        return self._client

    async def get_scores(self, force: bool = False) -> list[dict]:
        now = datetime.now(timezone.utc)
        if not force and self._cache_time and (now - self._cache_time).total_seconds() < LIVE_SCORE_TTL_SECONDS:
            next_reassess = self._cache_time + timedelta(seconds=LIVE_SCORE_TTL_SECONDS)
            return [self._with_freshness_meta(s, source_status="live", next_live_reassess_utc=next_reassess).to_dict() for s in self._cache.values()]

        results: list[ThreatScore] = []
        for monitor in MONITORS:
            score = await self._assess(monitor)
            results.append(score)
            self._cache[monitor.id] = score

        self._cache_time = now
        next_reassess = now + timedelta(seconds=LIVE_SCORE_TTL_SECONDS)
        results = [self._with_freshness_meta(s, source_status=s.source_status, next_live_reassess_utc=next_reassess) for s in results]
        return [s.to_dict() for s in results]

    async def _assess(self, m: ThreatMonitor) -> ThreatScore:
        source_errors: list[str] = []
        esc_articles: list[dict] = []
        deesc_articles: list[dict] = []

        tasks = [
            ("google_news_esc", self._query_google_news(m.escalation_query, m.lookback_hours, "google_news", "escalation"), "esc"),
            ("google_news_deesc", self._query_google_news(m.deescalation_query, m.lookback_hours, "google_news", "deescalation"), "deesc"),
            ("newsapi_esc", self._query_newsapi(m.escalation_query, m.lookback_hours, "escalation"), "esc"),
            ("newsapi_deesc", self._query_newsapi(m.deescalation_query, m.lookback_hours, "deescalation"), "deesc"),
            ("treasury_press", self._query_treasury_press(m), "esc"),
            ("defense_rss", self._query_defense_releases(m), "esc"),
            ("state_press", self._query_state_press(m), "both"),
            ("ofac_recent", self._query_ofac_recent_actions(m), "esc"),
        ]
        results = await asyncio_gather_named(tasks)
        for name, channel, result in results:
            if isinstance(result, Exception):
                logger.warning(f"[GeoRisk] source failed ({name}): {result}")
                source_errors.append(name)
                continue
            if channel == "esc":
                esc_articles.extend(result)
            elif channel == "deesc":
                deesc_articles.extend(result)
            else:
                for art in result:
                    kind = (art.get("kind") or "").lower()
                    if kind == "deescalation":
                        deesc_articles.append(art)
                    else:
                        esc_articles.append(art)

        esc_articles = self._dedupe_articles(esc_articles)
        deesc_articles = self._dedupe_articles(deesc_articles)

        now = datetime.now(timezone.utc)
        esc_count = len(esc_articles)
        deesc_count = len(deesc_articles)
        total = esc_count + deesc_count

        signal_sources = self._signal_source_rollup(esc_articles, deesc_articles, source_errors=source_errors)
        source_status = "live"
        source_note = None
        if total == 0 and source_errors:
            source_status = "delayed"
            source_note = "Assessing from partial source coverage while some official feeds are unavailable."
        elif total > 0 and source_errors:
            source_note = "Score computed from available low-latency sources; one or more supporting feeds were unavailable."

        if total == 0:
            if source_status != "live":
                return self._delayed_placeholder(m, signal_sources=signal_sources, note=source_note or "Assessing from limited coverage.")
            return self._empty_live_score(m, signal_sources=signal_sources)

        ratio = esc_count / total if total else 0.0
        volume_score = min(30, int(ratio * 37)) if total else 0
        background_context = self._background_context_floor(total)
        abs_score = min(25, int((esc_count / 20) * 25)) if esc_count > 0 else 0

        severity_hits = 0
        for art in esc_articles:
            title = art.get("title", "").lower()
            for kw in m.severity_keywords:
                if kw.lower() in title:
                    severity_hits += 1
                    break
        severity_ratio = (severity_hits / esc_count) if esc_count > 0 else 0.0
        severity_score = min(25, int(severity_ratio * 35)) if esc_count > 0 else 0

        recent_2h = 0
        recent_6h = 0
        for art in esc_articles:
            dt = self._parse_date(art.get("seendate", ""))
            if dt:
                age = (now - dt).total_seconds() / 3600
                if age <= 2:
                    recent_2h += 1
                if age <= 6:
                    recent_6h += 1
        recency_score = 0
        if recent_2h >= 5:
            recency_score = 20
        elif recent_2h >= 2:
            recency_score = 14
        elif recent_6h >= 5:
            recency_score = 10
        elif recent_6h >= 1:
            recency_score = 5

        risk_factors = self._extract_risk_factors(esc_articles, deesc_articles)
        factor_diversity_score = self._factor_diversity_score(risk_factors)
        source_breadth_score = self._source_breadth_score(esc_articles, deesc_articles)
        official_signal_score = self._official_signal_score(esc_articles)

        raw = volume_score + abs_score + severity_score + recency_score + factor_diversity_score + source_breadth_score + official_signal_score
        score = max(0, min(100, max(raw, background_context)))
        level = self._level(score)

        top_source = esc_articles if esc_articles else deesc_articles
        top = [
            {
                "title": art.get("title", ""),
                "url": art.get("url", ""),
                "domain": art.get("domain", ""),
                "seendate": art.get("seendate", ""),
                "source_family": art.get("source_family", "unknown"),
            }
            for art in top_source[:5]
        ]

        detail = self._build_detail(
            score=score,
            esc=esc_count,
            deesc=deesc_count,
            ratio=ratio,
            sev=severity_hits,
            r2h=recent_2h,
            r6h=recent_6h,
            background_context=background_context,
            total=total,
            risk_factors=risk_factors,
            source_breadth_score=source_breadth_score,
            signal_sources=signal_sources,
        )

        updated_utc = now.isoformat()
        return ThreatScore(
            id=m.id,
            label=m.label,
            description=m.description,
            score=score,
            level=level,
            detail=detail,
            escalation_articles=esc_count,
            deescalation_articles=deesc_count,
            coverage_articles=total,
            top_headlines=top,
            risk_factors=risk_factors,
            instruments=m.instruments,
            updated_utc=updated_utc,
            components={
                "volume_ratio": volume_score,
                "absolute_volume": abs_score,
                "severity_keywords": severity_score,
                "background_context": background_context,
                "recency": recency_score,
                "factor_diversity": factor_diversity_score,
                "source_breadth": source_breadth_score,
                "official_signals": official_signal_score,
                "esc_count": esc_count,
                "deesc_count": deesc_count,
                "ratio": round(ratio, 2),
                "severity_hits": severity_hits,
                "recent_2h": recent_2h,
                "recent_6h": recent_6h,
                "risk_factor_count": len(risk_factors),
                "source_count": len({(a.get('domain') or '').lower() for a in esc_articles + deesc_articles if a.get('domain')}),
                "source_family_count": len({(a.get('source_family') or '').lower() for a in esc_articles + deesc_articles if a.get('source_family')}),
                "failed_source_count": len(source_errors),
            },
            source_status=source_status,
            source_note=source_note,
            last_live_utc=updated_utc,
            signal_sources=signal_sources,
        )

    async def _query_google_news(self, query: str, hours: int, source_family: str, kind: str) -> list[dict]:
        client = await self._get_client()
        window_tag = "when:1d" if hours <= 24 else "when:7d"
        rss_query = quote_plus(f"{query} {window_tag}")
        resp = await client.get(f"{GOOGLE_NEWS_RSS}?q={rss_query}&hl=en-GB&gl=GB&ceid=GB:en")
        resp.raise_for_status()
        return self._parse_rss_articles(resp.text, source_family=source_family, cutoff_hours=hours, kind=kind)

    async def _query_newsapi(self, query: str, hours: int, kind: str) -> list[dict]:
        if not self.settings.newsapi_key:
            return []
        client = await self._get_client()
        since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        resp = await client.get(
            f"{self.settings.newsapi_base_url}/everything",
            params={
                "q": query,
                "from": since,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": 20,
                "apiKey": self.settings.newsapi_key,
            },
        )
        resp.raise_for_status()
        data = resp.json() if resp.text else {}
        out = []
        for art in data.get("articles", []) if isinstance(data, dict) else []:
            title = art.get("title") or ""
            if not title:
                continue
            published = art.get("publishedAt") or ""
            url = art.get("url") or ""
            domain = urlparse(url).netloc if url else (art.get("source") or {}).get("name", "newsapi")
            out.append({
                "title": title,
                "url": url,
                "domain": domain,
                "seendate": published,
                "source_family": "newsapi",
                "kind": kind,
            })
        return out

    async def _query_treasury_press(self, m: ThreatMonitor) -> list[dict]:
        client = await self._get_client()
        resp = await client.get(TREASURY_PRESS_URL)
        resp.raise_for_status()
        html = resp.text
        items = []
        keyword_patterns = self._official_keywords(m) + ["sanctions", "shadow fleet", "shipping", "oil", "export control", "embargo", "terror"]
        for href, title in self._extract_h3_links(html):
            title_l = title.lower()
            if not any(k in title_l for k in keyword_patterns):
                continue
            if not self._matches_monitor_region(title_l, m):
                continue
            items.append({
                "title": title,
                "url": urljoin(TREASURY_PRESS_URL, href),
                "domain": "treasury.gov",
                "seendate": datetime.now(timezone.utc).isoformat(),
                "source_family": "treasury_press",
                "kind": self._infer_kind(title_l),
            })
        return items[:8]

    async def _query_defense_releases(self, m: ThreatMonitor) -> list[dict]:
        client = await self._get_client()
        resp = await client.get(DEFENSE_RELEASES_RSS)
        resp.raise_for_status()
        articles = self._parse_rss_articles(resp.text, source_family="defense_rss", cutoff_hours=72, kind="escalation")
        keywords = self._official_keywords(m) + ["carrier", "destroyer", "deployment", "red sea", "hormuz", "strike", "missile"]
        return [
            a for a in articles
            if any(k in (a.get("title") or "").lower() for k in keywords)
            and self._matches_monitor_region((a.get("title") or "").lower(), m)
        ][:10]

    async def _query_state_press(self, m: ThreatMonitor) -> list[dict]:
        client = await self._get_client()
        resp = await client.get(STATE_PRESS_URL)
        resp.raise_for_status()
        html = resp.text
        items = []
        keywords = self._official_keywords(m) + ["ceasefire", "truce", "sanctions", "hostage", "humanitarian", "shipping"]
        for href, title in self._extract_anchor_links(html):
            title_l = title.lower()
            if not any(k in title_l for k in keywords):
                continue
            if not self._matches_monitor_region(title_l, m):
                continue
            items.append({
                "title": title,
                "url": urljoin(STATE_PRESS_URL, href),
                "domain": "state.gov",
                "seendate": datetime.now(timezone.utc).isoformat(),
                "source_family": "state_press",
                "kind": self._infer_kind(title_l),
            })
        return items[:10]

    async def _query_ofac_recent_actions(self, m: ThreatMonitor) -> list[dict]:
        client = await self._get_client()
        resp = await client.get(OFAC_RECENT_ACTIONS_URL)
        resp.raise_for_status()
        html = resp.text
        items = []
        keywords = self._official_keywords(m) + ["sanctions", "shadow fleet", "terrorism", "oil", "shipping", "iran", "hezbollah", "houthi", "hamas"]
        for href, title in self._extract_anchor_links(html):
            title_l = title.lower()
            if not any(k in title_l for k in keywords):
                continue
            if not self._matches_monitor_region(title_l, m):
                continue
            if not href.startswith("/") and "ofac.treasury.gov" not in href:
                continue
            items.append({
                "title": title,
                "url": urljoin(OFAC_RECENT_ACTIONS_URL, href),
                "domain": "ofac.treasury.gov",
                "seendate": datetime.now(timezone.utc).isoformat(),
                "source_family": "ofac_recent",
                "kind": self._infer_kind(title_l),
            })
        return items[:10]

    @staticmethod
    def _official_keywords(m: ThreatMonitor) -> list[str]:
        kws = [a.lower() for a in m.key_actors]
        kws += ["iran", "israel", "gaza", "hezbollah", "houthi", "red sea", "hormuz", "sanctions"]
        return sorted(set(kws))

    @staticmethod
    def _monitor_region_keywords(m: ThreatMonitor) -> list[str]:
        kws = {a.lower() for a in m.key_actors if a.lower() not in {"united states", "pentagon"}}
        kws.update({
            "middle east", "iran", "iranian", "israel", "israeli", "gaza", "west bank", "palestinian",
            "hamas", "hezbollah", "lebanon", "lebanese", "houthi", "houthis", "yemen", "yemeni",
            "syria", "syrian", "iraq", "iraqi", "red sea", "gulf of aden", "strait of hormuz", "hormuz",
            "tehran", "tel aviv", "jerusalem", "idf", "irgc",
        })
        return sorted(kws)

    def _matches_monitor_region(self, text_l: str, m: ThreatMonitor) -> bool:
        return any(k in text_l for k in self._monitor_region_keywords(m))

    @staticmethod
    def _infer_kind(title_l: str) -> str:
        deescalation_tokens = ["ceasefire", "truce", "negotiation", "diplomatic", "talks", "agreement", "mediation", "humanitarian"]
        if any(t in title_l for t in deescalation_tokens):
            conflict_tokens = ["strike", "missile", "killed", "retaliation", "attack", "shelling", "casualties"]
            if not any(t in title_l for t in conflict_tokens):
                return "deescalation"
        return "escalation"

    @staticmethod
    def _extract_h3_links(html: str) -> list[tuple[str, str]]:
        pattern = re.compile(r"<h3[^>]*>\s*<a[^>]+href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>\s*</h3>", re.I | re.S)
        links = []
        for href, raw in pattern.findall(html):
            title = re.sub(r"<[^>]+>", "", raw)
            title = re.sub(r"\s+", " ", unescape(title)).strip()
            if title:
                links.append((href, title))
        return links

    @staticmethod
    def _extract_anchor_links(html: str) -> list[tuple[str, str]]:
        pattern = re.compile(r"<a[^>]+href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", re.I | re.S)
        links = []
        seen = set()
        for href, raw in pattern.findall(html):
            title = re.sub(r"<[^>]+>", "", raw)
            title = re.sub(r"\s+", " ", unescape(title)).strip()
            if not title or len(title) < 12:
                continue
            key = (href, title)
            if key in seen:
                continue
            seen.add(key)
            links.append((href, title))
        return links

    def _parse_rss_articles(self, xml_text: str, *, source_family: str, cutoff_hours: int, kind: str) -> list[dict]:
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return []
        out = []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=cutoff_hours)
        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or item.findtext("published") or "").strip()
            source_name = ""
            source_node = item.find("source")
            if source_node is not None and source_node.text:
                source_name = source_node.text.strip()
            dt = self._parse_rfc822(pub_date) or self._parse_date(pub_date)
            if dt and dt < cutoff:
                continue
            domain = source_name or (urlparse(link).netloc if link else source_family)
            out.append({
                "title": title,
                "url": link,
                "domain": domain,
                "seendate": dt.isoformat() if dt else datetime.now(timezone.utc).isoformat(),
                "source_family": source_family,
                "kind": kind,
            })
        return out

    @staticmethod
    def _parse_rfc822(ds: str) -> Optional[datetime]:
        if not ds:
            return None
        fmts = ["%a, %d %b %Y %H:%M:%S %Z", "%a, %d %b %Y %H:%M:%S %z", "%d %b %Y %H:%M:%S %Z"]
        for fmt in fmts:
            try:
                dt = datetime.strptime(ds, fmt)
                return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
            except ValueError:
                continue
        return None

    @staticmethod
    def _dedupe_articles(articles: list[dict]) -> list[dict]:
        keep: dict[str, dict] = {}
        for art in articles:
            title = re.sub(r"\W+", " ", (art.get("title") or "").lower()).strip()
            if not title:
                continue
            existing = keep.get(title)
            if not existing:
                keep[title] = art
                continue
            current_rank = SOURCE_PRIORITY.get((art.get("source_family") or "").lower(), 9)
            existing_rank = SOURCE_PRIORITY.get((existing.get("source_family") or "").lower(), 9)
            current_dt = GeopoliticalRiskService._parse_date(art.get("seendate", "")) or datetime.fromtimestamp(0, tz=timezone.utc)
            existing_dt = GeopoliticalRiskService._parse_date(existing.get("seendate", "")) or datetime.fromtimestamp(0, tz=timezone.utc)
            if (current_rank, -current_dt.timestamp()) < (existing_rank, -existing_dt.timestamp()):
                keep[title] = art
        return sorted(keep.values(), key=lambda a: GeopoliticalRiskService._parse_date(a.get("seendate", "")) or datetime.fromtimestamp(0, tz=timezone.utc), reverse=True)

    @staticmethod
    def _signal_source_rollup(esc_articles: list[dict], deesc_articles: list[dict], *, source_errors: list[str]) -> list[dict]:
        rollup: dict[str, dict] = {}
        for art in esc_articles + deesc_articles:
            family = (art.get("source_family") or "unknown").lower()
            entry = rollup.setdefault(family, {"name": family, "count": 0, "status": "live"})
            entry["count"] += 1
        labels = {
            "google_news": "Google News",
            "newsapi": "NewsAPI",
            "treasury_press": "Treasury press",
            "defense_rss": "Defense releases",
            "state_press": "State press",
            "ofac_recent": "OFAC recent actions",
            "unknown": "Other",
        }
        aliases = {
            "google_news_esc": "google_news",
            "google_news_deesc": "google_news",
            "newsapi_esc": "newsapi",
            "newsapi_deesc": "newsapi",
        }
        for err in source_errors:
            fam = aliases.get(err, err)
            entry = rollup.setdefault(fam, {"name": fam, "count": 0, "status": "assessing"})
            entry["status"] = "assessing"
        order = ["defense_rss", "state_press", "treasury_press", "ofac_recent", "newsapi", "google_news", "unknown"]
        nice = []
        for key in order:
            if key in rollup:
                item = rollup[key]
                nice.append({"name": labels.get(key, key.title()), "count": item["count"], "status": item.get("status", "live")})
        return nice

    @staticmethod
    def _with_source_state(score: ThreatScore, status: str, note: str) -> ThreatScore:
        score.source_status = status
        score.source_note = note
        return score

    @staticmethod
    def _with_freshness_meta(score: ThreatScore, *, source_status: Optional[str] = None, next_live_reassess_utc: Optional[datetime] = None) -> ThreatScore:
        if source_status:
            score.source_status = source_status
        if next_live_reassess_utc:
            score.next_live_reassess_utc = next_live_reassess_utc.isoformat()
        if not score.last_live_utc and score.source_status == "live":
            score.last_live_utc = score.updated_utc
        return score

    def _empty_live_score(self, m: ThreatMonitor, signal_sources: Optional[list[dict]] = None) -> ThreatScore:
        now = datetime.now(timezone.utc).isoformat()
        return ThreatScore(
            id=m.id,
            label=m.label,
            description=m.description,
            score=0,
            level="Low",
            detail="Low escalation signal from current official and headline coverage. No immediate threat to technical trading conditions.",
            escalation_articles=0,
            deescalation_articles=0,
            coverage_articles=0,
            top_headlines=[],
            risk_factors=[],
            instruments=m.instruments,
            updated_utc=now,
            components={"volume_ratio": 0, "absolute_volume": 0, "severity_keywords": 0, "background_context": 0, "recency": 0, "factor_diversity": 0, "source_breadth": 0, "official_signals": 0, "esc_count": 0, "deesc_count": 0, "ratio": 0.0, "severity_hits": 0, "recent_2h": 0, "recent_6h": 0, "risk_factor_count": 0, "source_count": 0, "source_family_count": 0, "failed_source_count": 0},
            source_status="live",
            last_live_utc=now,
            signal_sources=signal_sources or [],
        )

    @staticmethod
    def _delayed_placeholder(m: ThreatMonitor, signal_sources: list[dict], note: str) -> ThreatScore:
        now = datetime.now(timezone.utc).isoformat()
        return ThreatScore(
            id=m.id,
            label=m.label,
            description=m.description,
            score=0,
            level="Low",
            detail=note,
            escalation_articles=0,
            deescalation_articles=0,
            coverage_articles=0,
            top_headlines=[],
            risk_factors=[],
            instruments=m.instruments,
            updated_utc=now,
            components={"volume_ratio": 0, "absolute_volume": 0, "severity_keywords": 0, "background_context": 0, "recency": 0, "factor_diversity": 0, "source_breadth": 0, "official_signals": 0, "esc_count": 0, "deesc_count": 0, "ratio": 0.0, "severity_hits": 0, "recent_2h": 0, "recent_6h": 0, "risk_factor_count": 0, "source_count": 0, "source_family_count": 0, "failed_source_count": len([s for s in signal_sources if s.get('status') != 'live'])},
            source_status="delayed",
            source_note=note,
            signal_sources=signal_sources,
        )

    @staticmethod
    def _background_context_floor(total_articles: int) -> int:
        if total_articles >= 60:
            return 16
        if total_articles >= 40:
            return 12
        if total_articles >= 20:
            return 8
        if total_articles >= 10:
            return 5
        if total_articles >= 5:
            return 3
        return 0

    @staticmethod
    def _level(score: int) -> str:
        if score >= 70:
            return "Critical"
        if score >= 45:
            return "High"
        if score >= 25:
            return "Elevated"
        return "Low"

    @staticmethod
    def _factor_diversity_score(risk_factors: list[dict]) -> int:
        active = {f["label"] for f in risk_factors if f.get("kind") in {"escalation", "market", "pressure"}}
        count = len(active)
        if count >= 4:
            return 10
        if count == 3:
            return 8
        if count == 2:
            return 5
        if count == 1:
            return 2
        return 0

    @staticmethod
    def _source_breadth_score(esc_articles: list[dict], deesc_articles: list[dict]) -> int:
        families = {(a.get("source_family") or "").lower() for a in esc_articles + deesc_articles if a.get("source_family")}
        domains = {(a.get("domain") or "").lower() for a in esc_articles + deesc_articles if a.get("domain")}
        score = 0
        if len(families) >= 4:
            score += 6
        elif len(families) >= 2:
            score += 4
        elif len(families) == 1:
            score += 2
        if len(domains) >= 8:
            score += 4
        elif len(domains) >= 4:
            score += 2
        elif len(domains) >= 2:
            score += 1
        return min(score, 10)

    @staticmethod
    def _official_signal_score(esc_articles: list[dict]) -> int:
        official = [a for a in esc_articles if (a.get("source_family") or "").lower() in {"treasury_press", "defense_rss", "state_press", "ofac_recent"}]
        if len(official) >= 3:
            return 6
        if len(official) >= 1:
            return 3
        return 0

    @staticmethod
    def _article_key(article: dict) -> str:
        title = re.sub(r"\W+", " ", (article.get("title") or "").lower()).strip()
        if title:
            return f"title:{title}"
        url = (article.get("url") or "").strip().lower()
        return f"url:{url}"

    @staticmethod
    def _match_count(title_l: str, patterns: list[str]) -> int:
        return sum(1 for p in patterns if p in title_l)

    def _best_factor_spec(self, title_l: str) -> dict | None:
        matches = []
        for spec in FACTOR_SPECS:
            count = self._match_count(title_l, spec["patterns"])
            if count:
                longest = max(len(p) for p in spec["patterns"] if p in title_l)
                matches.append((KIND_PRIORITY.get(spec["kind"], 9), -count, -longest, spec["label"], spec))
        if not matches:
            return None
        matches.sort()
        return matches[0][-1]

    def _extract_risk_factors(self, esc_articles: list[dict], deesc_articles: list[dict]) -> list[dict]:
        factors: dict[str, dict] = {}
        merged_articles: dict[str, dict] = {}

        def merge_articles(articles: list[dict], channel: str) -> None:
            for art in articles:
                title = (art.get("title") or "").strip()
                if not title:
                    continue
                key = self._article_key(art)
                current = merged_articles.get(key)
                rank = SOURCE_PRIORITY.get((art.get("source_family") or "").lower(), 9)
                seen = self._parse_date(art.get("seendate", "")) or datetime.now(timezone.utc)
                if not current:
                    current = {**art, "channels": set()}
                    current["_rank"] = rank
                    current["_seen_dt"] = seen
                    merged_articles[key] = current
                else:
                    if (rank, -seen.timestamp()) < (current.get("_rank", 9), -(current.get("_seen_dt") or datetime.fromtimestamp(0, tz=timezone.utc)).timestamp()):
                        channels = current.get("channels", set())
                        current = {**art, "channels": channels}
                        current["_rank"] = rank
                        current["_seen_dt"] = seen
                        merged_articles[key] = current
                    else:
                        current.setdefault("_seen_dt", seen)
                merged_articles[key].setdefault("channels", set()).add(channel)

        merge_articles(esc_articles, "esc")
        merge_articles(deesc_articles, "deesc")

        for art in merged_articles.values():
            title = (art.get("title") or "").strip()
            if not title:
                continue
            title_l = title.lower()
            seen = art.get("_seen_dt") or self._parse_date(art.get("seendate", "")) or datetime.now(timezone.utc)
            spec = self._best_factor_spec(title_l)
            if spec:
                label = spec["label"]
                kind = spec["kind"]
            else:
                if "esc" in art.get("channels", set()):
                    label = "Conflict coverage active"
                    kind = "context"
                else:
                    label = "Diplomatic activity"
                    kind = "context"
            current = factors.get(label)
            if not current:
                current = {
                    "label": label,
                    "kind": kind,
                    "count": 0,
                    "latest_utc": seen.isoformat(),
                    "latest_title": title,
                    "source_families": set(),
                }
                factors[label] = current
            current["count"] += 1
            current["source_families"].add((art.get("source_family") or "unknown").lower())
            latest_dt = self._parse_date(current["latest_utc"]) if current.get("latest_utc") else None
            if not latest_dt or seen > latest_dt:
                current["latest_utc"] = seen.isoformat()
                current["latest_title"] = title

        def sort_key(item: dict):
            dt = self._parse_date(item.get("latest_utc", "")) or datetime.fromtimestamp(0, tz=timezone.utc)
            return (dt, -KIND_PRIORITY.get(item.get("kind", "context"), 9), item.get("count", 0), len(item.get("source_families", [])))

        ordered = sorted(factors.values(), key=sort_key, reverse=True)
        out = []
        for item in ordered[:8]:
            fams = sorted(item.get("source_families", []))
            item["source_families"] = fams
            item["source_count"] = len(fams)
            out.append(item)
        return out

    @staticmethod
    def _factor_snippet(risk_factors: list[dict]) -> str:
        active = [f["label"] for f in risk_factors if f.get("kind") in {"escalation", "market", "pressure"}][:2]
        if active:
            return "; ".join(active)
        passive = [f["label"] for f in risk_factors][:2]
        return "; ".join(passive)

    def _build_detail(
        self,
        *,
        score: int,
        esc: int,
        deesc: int,
        ratio: float,
        sev: int,
        r2h: int,
        r6h: int,
        background_context: int,
        total: int,
        risk_factors: list[dict],
        source_breadth_score: int,
        signal_sources: list[dict],
    ) -> str:
        factor_snippet = self._factor_snippet(risk_factors)
        factor_suffix = f" Recent factors: {factor_snippet}." if factor_snippet else ""
        sources = ", ".join([s["name"] for s in signal_sources if s.get("count")])
        source_suffix = f" Signal sources: {sources}." if sources else ""
        if score >= 70:
            return (
                f"Critical escalation signal. {esc} conflict articles vs {deesc} diplomacy articles in 24h "
                f"(ratio {ratio:.0%}). {sev} contain high-severity language. {r2h} published in last 2h."
                f"{factor_suffix}{source_suffix} Stand aside from all risk positions."
            )
        if score >= 45:
            return (
                f"High escalation signal. {esc} conflict articles detected, {sev} with severe language. "
                f"{r6h} articles in last 6h.{factor_suffix} Avoid new entries in oil, gold, and equity futures.{source_suffix}"
            )
        if score >= 25:
            return (
                f"Elevated tension. {esc} conflict articles vs {deesc} diplomacy coverage. "
                f"Monitor closely — situation could escalate quickly.{factor_suffix} Cross-source confirmation score {source_breadth_score}/10.{source_suffix}"
            )
        if background_context > 0 and total > 0:
            return (
                f"No fresh escalation burst detected, but the flashpoint remains active in the news cycle: "
                f"{esc} conflict-style articles vs {deesc} diplomacy articles in 24h. "
                f"Treat this as background geopolitical tension, not a literal zero-risk reading.{factor_suffix}{source_suffix}"
            )
        return (
            f"Low escalation signal. {esc} conflict articles vs {deesc} diplomacy articles. "
            f"No immediate threat to technical trading conditions.{factor_suffix}{source_suffix}"
        )

    @staticmethod
    def _parse_date(ds: str) -> Optional[datetime]:
        if not ds:
            return None
        ds = ds.strip().replace(" ", "")
        try:
            if "T" in ds and ("+" in ds or ds.endswith("Z")):
                return datetime.fromisoformat(ds.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            pass
        for fmt in ["%Y%m%dT%H%M%SZ", "%Y%m%dT%H%M%S", "%Y-%m-%dT%H:%M:%SZ", "%Y%m%d%H%M%S", "%Y-%m-%dT%H:%M:%S"]:
            try:
                return datetime.strptime(ds, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()


async def asyncio_gather_named(tasks: list[tuple[str, object, str]]) -> list[tuple[str, str, object]]:
    coros = [coro for _, coro, _ in tasks]
    results = await asyncio.gather(*coros, return_exceptions=True)
    return [(name, channel, result) for (name, _, channel), result in zip(tasks, results)]
