# Market Event Risk Guard v2.3.1

A production-ready event-risk filter for technical traders. Identifies when external events could distort normal price behaviour, making technical indicators unreliable.

**Not a news dashboard.** A materiality-scored risk filter answering: *"Is it safe to rely on technicals right now?"*

---

## What It Does

For a rolling window (default 8h):

1. Aggregates events from 14 independent sources (macro calendars, earnings, crypto events, breaking news, Fed/SEC/ECB feeds, exchange status, Treasury auctions, geopolitical monitoring)
2. Scores each event for materiality using a transparent rule-based engine
3. Classifies events as **scheduled**, **breaking**, or **ongoing**
4. Assigns caution level: **Low / Moderate / High / Extreme**
5. Produces a verdict: **Yes** (technicals usable) / **Caution** / **No** (stand aside)
6. Highlights imminent distortion-risk events in a "Do Not Trust Technicals" section

---

## Quick Start (Local)

```bash
git clone https://github.com/YOUR_USER/market-event-risk-guard.git
cd market-event-risk-guard
cp .env.example .env
# Edit .env — add your API keys
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

Open http://localhost:8000

---

## Deploy to Render

1. Push repo to GitHub
2. In Render, create **New Blueprint** from the repo — reads `render.yaml`
3. Choose at minimum **Starter** plan
4. Add API keys as env vars (bulk-import from `.env.example`)
5. Deploy
6. Verify: `/health`, `/api/events`, `/api/summary`

---

## Environment Variables

See `.env.example`. Key variables:

| Variable | Purpose |
|---|---|
| `TRADINGECONOMICS_CLIENT_KEY` / `_SECRET` | Macro calendar |
| `FMP_API_KEY` | Earnings calendar |
| `COINMARKETCAL_API_KEY` | Crypto event calendar |
| `NEWSAPI_KEY` | Breaking news |
| `ENABLE_*_ADAPTER` | Toggle any source |
| `DEFAULT_WINDOW_HOURS` | Lookahead (default: 8) |
| `REFRESH_INTERVAL_SECONDS` | Auto-refresh (default: 60) |

Adapters with no key skip gracefully — the app never crashes due to missing keys.

---

## API Endpoints

| Endpoint | Params | Returns |
|---|---|---|
| `GET /health` | — | App status, per-source health |
| `GET /api/events` | `hours`, `asset_class`, `severity`, `classification` | Scored, filtered events |
| `GET /api/summary` | `hours` | Verdict (Yes/Caution/No), environment label, banner alert |

---

## Scoring Logic

Transparent additive framework:

1. **Base weight** by event type (FOMC=72, CPI=65, earnings=45, etc.)
2. **Breadth** +8 cross-asset, +10 systemic
3. **Surprise** +10 breaking, +4 ongoing
4. **Proximity** +12 within 15min, +8 within 30min, +5 within 60min
5. **Breaking premium** +6
6. **Regime-shift premium** +8 for war, sanctions, depeg, etc.

### Thresholds

| Score | Level | Action |
|---|---|---|
| 0–34 | Low | Technicals usable |
| 35–54 | Moderate | Use caution |
| 55–74 | High | Avoid new entries near event |
| 75–100 | Extreme | Stand aside until event passes |

### Caution Windows

- **Extreme**: 45min before → 60min after
- **High**: 30min → 45min
- **Moderate**: 15min → 30min
- **Low**: 5min → 15min

---

## Source Adapters

| Adapter | Key Required | Coverage |
|---|---|---|
| TradingEconomicsAdapter | Yes | CPI, NFP, GDP, PMI, rate decisions |
| FmpEarningsAdapter | Yes | Earnings, mega-cap detection |
| CoinMarketCalAdapter | Yes | Forks, upgrades, listings |
| NewsApiBreakingAdapter | Yes | Breaking news keyword classification |
| CoinbaseStatusAdapter | No | Coinbase incidents/maintenance |
| CoinbaseExchangeStatusAdapter | No | CDP/Exchange status |
| FedCalendarAdapter | No | Fed calendar RSS |
| FedPressAdapter | No | Fed press releases RSS |
| SecNewsAdapter | No | SEC press releases RSS |

---

## Fallback Behaviour

- Any adapter failure → app continues with remaining sources
- Source health visible in dashboard footer and `/health`
- Confidence reduced for less reliable sources
- Events deduplicated across sources (higher score kept)
- Missing fields handled with sensible defaults
- Times normalised to UTC, displayed in user's local timezone

---

## Testing

```bash
pip install -r requirements.txt
pytest tests/ -v
```

---

## Repo Structure

```
market-event-risk-guard/
├── app/
│   ├── adapters/
│   │   ├── base.py, tradingeconomics.py, fmp_earnings.py
│   │   ├── coinmarketcal.py, newsapi.py, coinbase_status.py
│   │   ├── fed.py, sec.py
│   ├── static/ (style.css, app.js)
│   ├── templates/ (index.html)
│   ├── config.py, main.py, models.py, scoring.py, service.py
├── tests/
├── .env.example, Dockerfile, render.yaml, requirements.txt
```


## v2 geopolitical monitor upgrades

- Adds **dynamic recent risk factors** to the threat card so the panel explains *why* the score is where it is.
- Uses **factor diversity** and **source breadth** as part of the score explanation.
- Keeps the earlier protections against false-zero readings during diplomacy-heavy or rate-limited periods.

## High-value future source additions

These are the next sources worth adding to make the geopolitical score more insightful without turning it into noise:

- **ACLED** for structured conflict-event data rather than headlines alone.
- **UKMTO / maritime security feeds** for Red Sea / Strait of Hormuz shipping disruption signals.
- **OFAC / sanctions notices** for direct sanctions-escalation inputs.
- **Official defence ministry / Pentagon / IDF / IRGC-facing briefings** for posture shifts.
- **Energy infrastructure and tanker-flow data** for oil-supply risk context.
- **Options-implied volatility and cross-asset stress indicators** to show whether markets are actually pricing the geopolitical move.


## v2.3 geopolitical change

- Removed GDELT from score-setting and event-refresh orchestration.
- Geopolitical scoring now uses low-latency sources: Google News RSS, NewsAPI (optional), Treasury press, Defense releases, State press, and OFAC recent actions.
- Goal: more stable scores, fewer rate-limit bottlenecks, and clearer official-source weighting.
