"""
Purplle SIS — FastAPI Real-Time Intelligence API (MongoDB edition)

Endpoints:
  GET  /health
  GET  /metrics                          (Prometheus)
  GET  /api/v1/store/occupancy
  GET  /api/v1/store/footfall?window=60m
  GET  /api/v1/zones/{zone_id}/dwell
  GET  /api/v1/anomalies?severity=&limit=
  GET  /api/v1/heatmap/{zone_id}?resolution=5m
  POST /api/v1/alerts/subscribe
  WS   /ws/v1/live
"""
import asyncio
import os
import random
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from prometheus_fastapi_instrumentator import Instrumentator

from src.db import get_async_db, ensure_indexes, get_sync_db

# ── App setup ──────────────────────────────────────────────────────────────────
app = FastAPI(title="Purplle Store Intelligence API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Wire Prometheus metrics at /metrics
Instrumentator().instrument(app).expose(app)

# ── WebSocket connection manager ──────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self._sockets: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self._sockets.append(ws)

    def disconnect(self, ws: WebSocket):
        self._sockets.remove(ws)

    async def broadcast(self, msg: dict):
        dead = []
        for ws in self._sockets:
            try:
                await ws.send_json(msg)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self._sockets.remove(ws)

manager = ConnectionManager()

# ── Models ─────────────────────────────────────────────────────────────────────
class AlertSub(BaseModel):
    url: str
    events: list[str]
    threshold: str = "warning"

# ── Helpers ────────────────────────────────────────────────────────────────────
def parse_window(window: str) -> timedelta:
    if window.endswith("m"):  return timedelta(minutes=int(window[:-1]))
    if window.endswith("h"):  return timedelta(hours=int(window[:-1]))
    if window.endswith("d"):  return timedelta(days=int(window[:-1]))
    return timedelta(minutes=int(window))

def _serialize(doc: dict) -> dict:
    """Convert MongoDB ObjectId / datetime to JSON-safe types."""
    doc.pop("_id", None)
    for k, v in doc.items():
        if isinstance(v, datetime):
            doc[k] = v.isoformat() + "Z"
    return doc

# ── Startup ────────────────────────────────────────────────────────────────────
_db_available = False

@app.on_event("startup")
async def startup():
    global _db_available
    try:
        db = get_sync_db()
        ensure_indexes(db)
        db.command("ping")
        _db_available = True
        print("[startup] MongoDB connected.")
    except Exception as exc:
        _db_available = False
        print(f"[startup] MongoDB unavailable ({exc}); falling back to sample data.")

# ── Health ─────────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status": "ok",
        "ts":     datetime.now(timezone.utc).isoformat(),
        "db":     "mongodb" if _db_available else "fallback",
    }

# ── Occupancy ──────────────────────────────────────────────────────────────────
@app.get("/api/v1/store/occupancy")
async def occupancy():
    if _db_available:
        db = get_async_db()
        zones = {}
        async for doc in db.occupancy_cache.find():
            zones[doc["zone"]] = doc["count"]
        if zones:
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "total":     sum(zones.values()),
                "zones":     zones,
            }
    # Fallback
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total": 4,
        "zones": {
            "skincare_aisle":  2,
            "makeup_aisle":    1,
            "billing_counter": 1,
            "store_entrance":  0,
            "stockroom":       0,
        },
    }

# ── Footfall ───────────────────────────────────────────────────────────────────
@app.get("/api/v1/store/footfall")
async def footfall(window: str = Query("60m")):
    since = datetime.now(timezone.utc) - parse_window(window)
    if _db_available:
        db = get_async_db()
        pipeline = [
            {"$match": {"event_type": {"$in": ["person_entry", "person_exit"]},
                        "timestamp": {"$gte": since}}},
            {"$group": {"_id": "$event_type", "count": {"$sum": 1}}},
        ]
        counts = {}
        async for doc in db.events.aggregate(pipeline):
            counts[doc["_id"]] = doc["count"]

        entries = counts.get("person_entry", 0)
        exits   = counts.get("person_exit",  0)

        # Peak occupancy in window
        peak_pipeline = [
            {"$match": {"event_type": "occupancy_update",
                        "timestamp": {"$gte": since}}},
            {"$group": {"_id": None,
                        "peak": {"$max": "$data.person_count"},
                        "peak_at": {"$last": "$timestamp"}}},
        ]
        peak_occupancy, peak_at = 0, None
        async for doc in db.events.aggregate(peak_pipeline):
            peak_occupancy = doc.get("peak", 0) or 0
            raw_ts = doc.get("peak_at")
            peak_at = raw_ts.isoformat() + "Z" if isinstance(raw_ts, datetime) else raw_ts

        return {"window": window, "entries": entries, "exits": exits,
                "net": entries - exits, "peak_occupancy": peak_occupancy, "peak_at": peak_at}

    return {"window": window, "entries": 18, "exits": 14, "net": 4,
            "peak_occupancy": 8, "peak_at": "2026-04-10T20:10:45Z"}

# ── Dwell time ─────────────────────────────────────────────────────────────────
@app.get("/api/v1/zones/{zone_id}/dwell")
async def dwell(zone_id: str):
    if _db_available:
        db = get_async_db()
        pipeline = [
            {"$match": {"zone": zone_id, "event_type": "occupancy_update",
                        "data.dwell_time_avg_s": {"$exists": True}}},
            {"$project": {"dwell": "$data.dwell_time_avg_s"}},
        ]
        values = []
        async for doc in db.events.aggregate(pipeline):
            v = doc.get("dwell")
            if v is not None:
                values.append(int(v))
        if values:
            values.sort()
            n = len(values)
            return {
                "zone_id":     zone_id,
                "avg_dwell_s": sum(values) // n,
                "p50":         values[int(n * 0.50)],
                "p90":         values[min(int(n * 0.90), n - 1)],
                "p99":         values[min(int(n * 0.99), n - 1)],
                "sample_size": n,
            }

    # Fallback
    sample = {
        "skincare_aisle":  {"avg_dwell_s": 52, "p50": 38, "p90": 120, "p99": 187, "sample_size": 14},
        "makeup_aisle":    {"avg_dwell_s": 68, "p50": 55, "p90": 130, "p99": 210, "sample_size": 11},
        "billing_counter": {"avg_dwell_s": 35, "p50": 28, "p90":  80, "p99": 120, "sample_size": 18},
    }
    if zone_id not in sample:
        raise HTTPException(404, detail=f"Zone '{zone_id}' not found")
    return {"zone_id": zone_id, **sample[zone_id]}

# ── Anomalies ──────────────────────────────────────────────────────────────────
@app.get("/api/v1/anomalies")
async def anomalies(severity: Optional[str] = None, limit: int = Query(10, le=100)):
    if _db_available:
        db = get_async_db()
        filt = {"severity": severity} if severity else {}
        results = []
        async for doc in db.anomalies.find(filt).sort("timestamp", -1).limit(limit):
            doc.pop("_id", None)
            if isinstance(doc.get("timestamp"), datetime):
                doc["timestamp"] = doc["timestamp"].isoformat() + "Z"
            results.append(doc)
        return {"anomalies": results, "total": len(results)}

    return {
        "anomalies": [
            {"anomaly_id": "ANO_001", "anomaly_type": "queue_buildup",          "severity": "critical",
             "camera_id": "CAM_5", "confidence": 0.92, "timestamp": "2026-04-10T20:11:15Z",
             "description": "Queue 6 > threshold 4"},
            {"anomaly_id": "ANO_002", "anomaly_type": "prolonged_dwell",        "severity": "warning",
             "camera_id": "CAM_1", "confidence": 0.81, "timestamp": "2026-04-10T20:10:35Z",
             "description": "Avg dwell 135s > threshold 120s in skincare_aisle"},
            {"anomaly_id": "ANO_003", "anomaly_type": "tailgating",             "severity": "warning",
             "camera_id": "CAM_3", "confidence": 0.68, "timestamp": "2026-04-10T20:10:20Z",
             "description": "3 entries detected in 1.8s"},
        ],
        "total": 3,
    }

# ── Heatmap ────────────────────────────────────────────────────────────────────
@app.get("/api/v1/heatmap/{zone_id}")
async def heatmap(zone_id: str, resolution: str = Query("5m")):
    if _db_available:
        db = get_async_db()
        series = []
        async for doc in (db.zone_heatmap.find({"zone": zone_id})
                          .sort("ts", -1).limit(20)):
            ts = doc["ts"]
            series.append({
                "ts":         (ts.isoformat() + "Z") if isinstance(ts, datetime) else ts,
                "motion_pct": doc["motion_pct"],
            })
        if series:
            return {"zone_id": zone_id, "resolution": resolution, "series": series[::-1]}

    return {
        "zone_id":    zone_id,
        "resolution": resolution,
        "series": [
            {"ts": f"2026-04-10T20:{i:02d}:00Z", "motion_pct": round(random.uniform(2, 16), 2)}
            for i in range(9, 13)
        ],
    }

# ── Alert subscription ─────────────────────────────────────────────────────────
@app.post("/api/v1/alerts/subscribe")
def subscribe(sub: AlertSub):
    return {
        "subscription_id": f"SUB_{hash(sub.url) & 0xFFFF:04X}",
        "status":          "active",
        "events":          sub.events,
    }

# ── WebSocket live feed ────────────────────────────────────────────────────────
@app.websocket("/ws/v1/live")
async def ws_live(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            if _db_available:
                db = get_async_db()
                zones = {}
                async for doc in db.occupancy_cache.find():
                    zones[doc["zone"]] = doc["count"]
                if zones:
                    await ws.send_json({
                        "event_type": "occupancy_update",
                        "zones":      zones,
                        "ts":         datetime.now(timezone.utc).isoformat(),
                    })
                    await asyncio.sleep(3)
                    continue

            # Fallback: synthetic push
            await ws.send_json({
                "event_type": "occupancy_update",
                "zone":       random.choice(["skincare_aisle", "makeup_aisle", "billing_counter"]),
                "count":      random.randint(0, 5),
                "ts":         datetime.now(timezone.utc).isoformat(),
            })
            await asyncio.sleep(3)
    except WebSocketDisconnect:
        manager.disconnect(ws)

# ── Dashboard (inline HTML) ────────────────────────────────────────────────────
@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard():
    """Serve the live store intelligence dashboard."""
    with open("dashboard/index.html", "r") as f:
        return f.read()
