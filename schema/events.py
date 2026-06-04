"""Purplle SIS — Canonical Event Schema (Pydantic v2)"""
from pydantic import BaseModel, Field
from typing import Literal, Optional, Union
from datetime import datetime, timezone
import uuid

EventType = Literal[
    "occupancy_update",
    "person_entry",
    "person_exit",
    "product_interaction",
    "staff_activity",
    "anomaly_detected",
    "queue_buildup",
    "prolonged_dwell",
    "unauthorized_access",
]

class TrackMeta(BaseModel):
    model: str = "motion_bg_sub"
    confidence: float
    frame_idx: int = 0

class OccupancyData(BaseModel):
    person_count: int
    dwell_time_avg_s: Optional[int] = None
    track_ids: list[str] = Field(default_factory=list)

class EntryExitData(BaseModel):
    direction: Literal["entry", "exit"]
    group_size: int = 1

class ProductData(BaseModel):
    shelf_section: str
    interaction_count: int
    track_id: Optional[str] = None

class AnomalyData(BaseModel):
    anomaly_type: str
    severity: Literal["critical", "warning", "info"]
    confidence: float
    description: str
    extra: dict = Field(default_factory=dict)

class StoreEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: f"EVT_{uuid.uuid4().hex[:8].upper()}")
    camera_id: str
    zone: str
    event_type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    data: Union[OccupancyData, EntryExitData, ProductData, AnomalyData, dict]
    meta: TrackMeta

    model_config = {"json_encoders": {datetime: lambda v: v.isoformat() + "Z"}}

    def to_mongo(self) -> dict:
        """Serialise to a dict safe for MongoDB insertion."""
        d = self.model_dump()
        # MongoDB stores datetime natively — keep it as datetime
        d["_id"] = d["event_id"]
        return d
