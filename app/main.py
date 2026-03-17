"""
Market Event Risk Guard — FastAPI application entry point.
"""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.config import get_settings
from app.service import EventService
from app.georisk import GeopoliticalRiskService

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("merg")

# ── Paths ────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent
TEMPLATE_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

# ── Service singletons ───────────────────────────────────────────────────────

event_service: EventService | None = None
georisk_service: GeopoliticalRiskService | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global event_service, georisk_service
    settings = get_settings()
    logger.info(f"Starting {settings.app_name} ({settings.app_env})")
    event_service = EventService()
    georisk_service = GeopoliticalRiskService()
    # Fire off the initial adapter refresh in the background.
    # /health reads whatever state exists (fast, never blocks).
    # /api/events waits for the refresh to finish via the lock.
    event_service.start_background_refresh()
    yield
    logger.info("Shutting down...")
    if event_service:
        await event_service.shutdown()
    if georisk_service:
        await georisk_service.close()


# ── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Market Event Risk Guard v2",
    version="2.0.0",
    lifespan=lifespan,
)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    settings = get_settings()
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "app_name": settings.app_name,
            "refresh_interval": settings.refresh_interval_seconds,
            "default_window": settings.default_window_hours,
        },
    )

@app.head("/")
async def head_index():
    return JSONResponse({"ok": True})


@app.get("/health")
async def health():
    settings = get_settings()
    if not event_service:
        return JSONResponse({"status": "starting", "app": settings.app_name}, status_code=503)

    # Health is always fast — it reads current state without triggering a refresh.
    # The background refresh populates adapter state during startup.
    sources = event_service.get_source_health()
    enabled = [s for s in sources if s.enabled]
    healthy = [s for s in enabled if s.healthy]

    if not event_service.has_refreshed:
        status = "starting"
    elif len(healthy) == len(enabled):
        status = "healthy"
    else:
        status = "degraded"

    return JSONResponse({
        "status": status,
        "app": settings.app_name,
        "env": settings.app_env,
        "utc_now": datetime.now(timezone.utc).isoformat(),
        "refreshing": event_service.is_refreshing,
        "sources": [s.model_dump(mode="json") for s in sources],
    })


@app.get("/api/events")
async def api_events(
    hours: Optional[int] = Query(default=None, ge=1, le=48),
    asset_class: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None),
    severity: Optional[str] = Query(default=None),
    classification: Optional[str] = Query(default=None),
):
    if not event_service:
        return JSONResponse({"error": "Service not ready"}, status_code=503)

    window = hours or get_settings().default_window_hours
    events = await event_service.get_events(
        hours=window,
        asset_class=asset_class,
        category=category,
        severity=severity,
        classification=classification,
    )
    # Recent lookback matches the selected window exactly.
    # If you select 1h, you see at most 1h of recent events.
    recent = await event_service.get_recent_events(
        hours=window,
        asset_class=asset_class,
        category=category,
        severity=severity,
        classification=classification,
    )
    return JSONResponse({
        "count": len(events),
        "recent_count": len(recent),
        "window_hours": window,
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "events": [e.model_dump(mode="json") for e in events],
        "recent_events": [e.model_dump(mode="json") for e in recent],
    })


@app.get("/api/summary")
async def api_summary(
    hours: Optional[int] = Query(default=None, ge=1, le=48),
    asset_class: Optional[str] = Query(default=None),
    category: Optional[str] = Query(default=None),
    severity: Optional[str] = Query(default=None),
    classification: Optional[str] = Query(default=None),
):
    if not event_service:
        return JSONResponse({"error": "Service not ready"}, status_code=503)

    summary = await event_service.get_summary(
        hours=hours,
        asset_class=asset_class,
        category=category,
        severity=severity,
        classification=classification,
    )
    summary["updated_utc"] = datetime.now(timezone.utc).isoformat()
    return JSONResponse(summary)


@app.get("/api/georisk")
async def api_georisk():
    if not georisk_service:
        return JSONResponse({"error": "Service not ready"}, status_code=503)

    scores = await georisk_service.get_scores()
    return JSONResponse({
        "threats": scores,
        "updated_utc": datetime.now(timezone.utc).isoformat(),
    })


@app.get("/api/debug")
async def api_debug(hours: Optional[int] = Query(default=8, ge=1, le=48)):
    """Compact diagnostic dump — paste this output to debug filtering issues."""
    if not event_service:
        return JSONResponse({"error": "Service not ready"}, status_code=503)

    now = datetime.now(timezone.utc)
    settings = get_settings()

    # Raw cache
    cache = event_service._cache
    cache_time = event_service._cache_time

    # Get filtered events (what the UI sees)
    events = await event_service.get_events(hours=hours)
    recent = await event_service.get_recent_events(hours=hours)

    def mini(e):
        return {
            "id": e.id,
            "title": e.title[:80],
            "start": e.start_time_utc.isoformat(),
            "class": e.classification.value,
            "score": e.impact_score,
            "caution": e.caution_level.value,
            "conf": e.confidence,
            "source": e.source_name,
            "cw_end": e.caution_window_end_utc.isoformat() if e.caution_window_end_utc else None,
            "age_min": round((now - e.start_time_utc).total_seconds() / 60, 1),
            "future_min": round((e.start_time_utc - now).total_seconds() / 60, 1),
        }

    return JSONResponse({
        "utc_now": now.isoformat(),
        "window_hours": hours,
        "cache_size": len(cache),
        "cache_updated": cache_time.isoformat() if cache_time else None,
        "filtered_event_count": len(events),
        "recent_count": len(recent),
        "events": [mini(e) for e in events],
        "recent": [mini(e) for e in recent[:10]],
        "sources": [
            {"name": s.name, "enabled": s.enabled, "healthy": s.healthy,
             "needs_key": s.needs_key, "count": s.event_count,
             "error": s.last_error}
            for s in event_service.get_source_health()
        ],
    })
