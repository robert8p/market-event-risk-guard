"""Normalised event schema and supporting enumerations."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ── Enums ────────────────────────────────────────────────────────────────────

class AssetClass(str, Enum):
    EQUITIES = "equities"
    CRYPTO = "crypto"
    BOTH = "both"


class Classification(str, Enum):
    SCHEDULED = "scheduled"
    BREAKING = "breaking"
    ONGOING = "ongoing"


class CautionLevel(str, Enum):
    LOW = "Low"
    MODERATE = "Moderate"
    HIGH = "High"
    EXTREME = "Extreme"


class SuggestedAction(str, Enum):
    TECHNICALS_USABLE = "Technicals usable"
    USE_CAUTION = "Use caution"
    AVOID_NEW_ENTRIES = "Avoid new entries near event"
    STAND_ASIDE = "Stand aside until event passes"


class EventCategory(str, Enum):
    MACRO = "macro"
    EQUITY = "equity"
    CRYPTO = "crypto"
    CROSS_ASSET = "cross_asset"
    SYSTEMIC = "systemic"


# ── Normalised Event ─────────────────────────────────────────────────────────

class NormalisedEvent(BaseModel):
    id: str
    title: str
    category: EventCategory
    event_type: str
    classification: Classification
    asset_class: AssetClass
    start_time_utc: datetime
    end_time_utc: Optional[datetime] = None
    countdown_minutes: Optional[float] = None
    impact_score: int = Field(ge=0, le=100)
    caution_level: CautionLevel
    suggested_action: SuggestedAction
    confidence: float = Field(ge=0.0, le=1.0, default=0.5)
    why_it_matters: str = ""
    source_name: str = ""
    source_url: str = ""
    affected_instruments: list[str] = Field(default_factory=list)
    caution_window_start_utc: Optional[datetime] = None
    caution_window_end_utc: Optional[datetime] = None
    data_quality_notes: Optional[str] = None
    description: str = ""


# ── Summary ──────────────────────────────────────────────────────────────────

class EnvironmentLabel(str, Enum):
    NORMAL = "Normal"
    EVENT_SENSITIVE = "Event-sensitive"
    HIGH_RISK = "High-risk for technical-only trading"


class RiskSummary(BaseModel):
    event_count: int = 0
    highest_caution: CautionLevel = CautionLevel.LOW
    environment_label: EnvironmentLabel = EnvironmentLabel.NORMAL
    verdict: str = "Yes"
    verdict_detail: str = "No major event risk detected."
    source_coverage: str = "full"  # full | partial | degraded | none
    imminent_events: list[NormalisedEvent] = Field(default_factory=list)
    ongoing_events: list[NormalisedEvent] = Field(default_factory=list)
    highest_distortion_events: list[NormalisedEvent] = Field(default_factory=list)
    banner_alert: Optional[str] = None
    window_hours: int = 8


# ── Source Health ────────────────────────────────────────────────────────────

class SourceHealth(BaseModel):
    name: str
    enabled: bool = True
    status: str = "pending"  # pending | healthy | failed | needs_key | disabled
    last_fetch_utc: Optional[datetime] = None
    last_error: Optional[str] = None
    event_count: int = 0

    @property
    def healthy(self) -> bool:
        return self.status == "healthy"

    @property
    def needs_key(self) -> bool:
        return self.status == "needs_key"

    def model_dump(self, **kwargs) -> dict:
        d = super().model_dump(**kwargs)
        d["healthy"] = self.healthy
        d["needs_key"] = self.needs_key
        return d
