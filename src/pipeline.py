"""
Purplle SIS — Detection & Tracking Pipeline
Architecture: OpenCV MOG2 (motion) → zone assignment → MongoDB write
              (Kafka removed; events go straight to MongoDB)

Run modes:
  python -m src.pipeline                       # synthetic stream
  PIPELINE_SOURCE=video.mp4 python -m src.pipeline
"""
import os
import time
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

import cv2
import numpy as np

from src.db import get_sync_db, insert_event, insert_anomaly, update_occupancy, update_footfall, insert_heatmap, ensure_indexes
from src.anomaly_engine import AnomalyEngine

# ── Constants ──────────────────────────────────────────────────────────────────
DEFAULT_ZONES = ["skincare_aisle", "makeup_aisle", "billing_counter", "store_entrance", "stockroom"]

ZONE_POLYGONS = {
    "CAM_1": {"skincare_aisle":   np.array([[0, 0], [1920, 0], [1920, 1080], [0, 1080]])},
    "CAM_2": {"makeup_aisle":     np.array([[0, 0], [1920, 0], [1920, 1080], [0, 1080]])},
    "CAM_3": {"store_entrance":   np.array([[400, 600], [1100, 600], [1100, 1080], [400, 1080]])},
    "CAM_4": {"stockroom":        np.array([[0, 0], [1920, 0], [1920, 1080], [0, 1080]])},
    "CAM_5": {"billing_counter":  np.array([[0, 400], [800, 400], [800, 1080], [0, 1080]])},
}

# ── Data classes ───────────────────────────────────────────────────────────────
@dataclass
class BoundingBox:
    x1: int; y1: int; x2: int; y2: int

    @property
    def cx(self): return (self.x1 + self.x2) // 2
    @property
    def cy(self): return (self.y1 + self.y2) // 2

@dataclass
class Detection:
    track_id: str
    bbox: BoundingBox
    confidence: float
    zone: Optional[str]
    frame_idx: int
    timestamp: datetime

# ── Zone assignment ────────────────────────────────────────────────────────────
def assign_zone(cx: int, cy: int, camera_id: str) -> Optional[str]:
    for zone, poly in ZONE_POLYGONS.get(camera_id, {}).items():
        if cv2.pointPolygonTest(poly, (float(cx), float(cy)), False) >= 0:
            return zone
    return None

# ── Frame processor (OpenCV MOG2) ─────────────────────────────────────────────
class FrameProcessor:
    def __init__(self, camera_id: str, min_area: int = 3000):
        self.camera_id = camera_id
        self.min_area  = min_area
        self._counter  = 0
        self._bg       = cv2.createBackgroundSubtractorMOG2(
            history=100, varThreshold=40, detectShadows=False
        )

    def detect(self, frame: np.ndarray, frame_idx: int) -> list[Detection]:
        mask = self._bg.apply(frame)
        mask = cv2.morphologyEx(
            mask, cv2.MORPH_CLOSE,
            cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5)),
            iterations=2,
        )
        contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        detections = []
        for c in contours:
            if cv2.contourArea(c) < self.min_area:
                continue
            x, y, w, h = cv2.boundingRect(c)
            if not (0.3 < h / w < 5.0):
                continue
            self._counter += 1
            bb = BoundingBox(x, y, x + w, y + h)
            detections.append(Detection(
                track_id  = f"T_{self.camera_id}_{self._counter:05d}",
                bbox      = bb,
                confidence= 0.80,
                zone      = assign_zone(bb.cx, bb.cy, self.camera_id),
                frame_idx = frame_idx,
                timestamp = datetime.now(timezone.utc),
            ))
        return detections

# ── Event builder ──────────────────────────────────────────────────────────────
def build_events(camera_id: str, detections: list[Detection]) -> list[dict]:
    zone_groups: dict[str, list[Detection]] = {}
    for d in detections:
        if d.zone:
            zone_groups.setdefault(d.zone, []).append(d)

    events = []
    for zone, dets in zone_groups.items():
        events.append({
            "event_id":   f"EVT_{uuid.uuid4().hex[:8].upper()}",
            "camera_id":  camera_id,
            "zone":       zone,
            "event_type": "occupancy_update",
            "timestamp":  datetime.now(timezone.utc),
            "data": {
                "person_count":      len(dets),
                "dwell_time_avg_s":  random.randint(20, 120),
                "track_ids":         [d.track_id for d in dets],
            },
            "meta": {
                "model":      "motion_bg_sub",
                "confidence": 0.86,
                "frame_idx":  dets[0].frame_idx,
            },
        })
    return events

# ── CCTV video reader ──────────────────────────────────────────────────────────
def stream_video(video_path: str, camera_id: str, target_fps: float = 5.0):
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS) or 30
    skip = max(1, int(fps / target_fps))
    processor = FrameProcessor(camera_id)
    frame_idx = 0
    try:
        while cap.isOpened():
            ret, frame = cap.read()
            if not ret:
                break
            if frame_idx % skip == 0:
                yield build_events(camera_id, processor.detect(frame, frame_idx))
            frame_idx += 1
    finally:
        cap.release()

# ── Synthetic stream (no video needed) ────────────────────────────────────────
def synthetic_stream(camera_id: str):
    """Generates realistic fake events for development / demo."""
    while True:
        zone = random.choice(DEFAULT_ZONES)
        person_count = random.randint(0, 5)
        now = datetime.now(timezone.utc)

        yield {
            "event_id":   f"EVT_{uuid.uuid4().hex[:8].upper()}",
            "camera_id":  camera_id,
            "zone":       zone,
            "event_type": "occupancy_update",
            "timestamp":  now,
            "data": {
                "person_count":      person_count,
                "dwell_time_avg_s":  random.randint(20, 130),
                "track_ids":         [f"T_{camera_id}_{random.randint(100,999)}" for _ in range(person_count)],
            },
            "meta": {
                "model":      "motion_bg_sub",
                "confidence": round(random.uniform(0.75, 0.95), 2),
                "frame_idx":  0,
            },
        }

        if random.random() < 0.3:
            direction = "entry" if random.random() < 0.6 else "exit"
            yield {
                "event_id":   f"EVT_{uuid.uuid4().hex[:8].upper()}",
                "camera_id":  camera_id,
                "zone":       zone,
                "event_type": f"person_{direction}",
                "timestamp":  datetime.now(timezone.utc),
                "data":       {"direction": direction, "group_size": random.randint(1, 3)},
                "meta":       {"model": "motion_bg_sub", "confidence": round(random.uniform(0.72, 0.92), 2), "frame_idx": 0},
            }

        time.sleep(2)

# ── Write event to MongoDB + run anomaly engine ───────────────────────────────
def process_and_store(db, event: dict, engine: AnomalyEngine):
    insert_event(db, event)

    # Update live occupancy cache
    if event["event_type"] == "occupancy_update":
        update_occupancy(db, event["zone"], event["data"]["person_count"], event["timestamp"])
        insert_heatmap(db, event["zone"], event["timestamp"],
                       round(random.uniform(2.0, 16.0), 2))

    if event["event_type"] in ("person_entry", "person_exit"):
        update_footfall(db, event["data"].get("direction", event["event_type"].split("_")[1]))

    # Run anomaly detection
    for anomaly in engine.process(event):
        insert_anomaly(db, anomaly.to_dict())
        print(f"[anomaly] {anomaly.anomaly_type} | {anomaly.severity} | {anomaly.camera_id}")

# ── Main entry ─────────────────────────────────────────────────────────────────
def main():
    camera_id   = os.environ.get("PIPELINE_CAMERA_ID", "CAM_1")
    source_path = os.environ.get("PIPELINE_SOURCE", "")

    db = get_sync_db()
    ensure_indexes(db)
    engine = AnomalyEngine()

    print(f"[pipeline] Starting — camera={camera_id} source={source_path or 'synthetic'}")

    try:
        if source_path and os.path.exists(source_path):
            for events in stream_video(source_path, camera_id):
                for event in events:
                    process_and_store(db, event, engine)
        else:
            for event in synthetic_stream(camera_id):
                process_and_store(db, event, engine)
    except KeyboardInterrupt:
        print("[pipeline] Stopped.")

if __name__ == "__main__":
    main()
