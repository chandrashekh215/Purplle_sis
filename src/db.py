"""
Purplle SIS — MongoDB Database Layer
Replaces: Kafka + Redis + TimescaleDB with a single MongoDB instance.

Collections:
  - events          : raw store events (indexed on timestamp, zone, event_type)
  - anomalies       : detected anomalies
  - zone_heatmap    : time-series motion data per zone
  - occupancy_cache : current live occupancy per zone (acts as Redis replacement)
  - footfall        : running entry/exit counters
"""
import os
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import DuplicateKeyError

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("MONGO_DB",  "purplle_sis")

# ── Sync client (used by pipeline + stream processor) ────────────────────────
_sync_client: MongoClient | None = None

def get_sync_db():
    global _sync_client
    if _sync_client is None:
        _sync_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    return _sync_client[DB_NAME]

# ── Async client (used by FastAPI) ────────────────────────────────────────────
_async_client: AsyncIOMotorClient | None = None

def get_async_db():
    global _async_client
    if _async_client is None:
        _async_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    return _async_client[DB_NAME]

# ── Index setup (run once at startup) ─────────────────────────────────────────
def ensure_indexes(db):
    """Create all indexes needed for fast queries."""
    # events: filter by time window, zone, event_type
    db.events.create_index([("timestamp", DESCENDING)])
    db.events.create_index([("zone", ASCENDING), ("timestamp", DESCENDING)])
    db.events.create_index([("event_type", ASCENDING), ("timestamp", DESCENDING)])
    db.events.create_index([("event_id", ASCENDING)], unique=True)

    # anomalies: filter by severity, time
    db.anomalies.create_index([("timestamp", DESCENDING)])
    db.anomalies.create_index([("severity", ASCENDING), ("timestamp", DESCENDING)])
    db.anomalies.create_index([("anomaly_id", ASCENDING)], unique=True)

    # heatmap: filter by zone + time
    db.zone_heatmap.create_index([("zone", ASCENDING), ("ts", DESCENDING)])

    # occupancy cache: zone is PK
    db.occupancy_cache.create_index([("zone", ASCENDING)], unique=True)

    print("[db] Indexes ensured.")

# ── Event helpers ─────────────────────────────────────────────────────────────
def insert_event(db, event: dict) -> bool:
    """Insert one event. Returns True on success, False on duplicate."""
    try:
        db.events.insert_one({**event, "_id": event["event_id"]})
        return True
    except DuplicateKeyError:
        return False

def insert_anomaly(db, anomaly: dict) -> bool:
    try:
        db.anomalies.insert_one({**anomaly, "_id": anomaly["anomaly_id"]})
        return True
    except DuplicateKeyError:
        return False

def insert_heatmap(db, zone: str, ts: datetime, motion_pct: float):
    db.zone_heatmap.insert_one({"zone": zone, "ts": ts, "motion_pct": motion_pct})

# ── Occupancy cache (replaces Redis HSET) ─────────────────────────────────────
def update_occupancy(db, zone: str, count: int, ts: datetime):
    db.occupancy_cache.update_one(
        {"zone": zone},
        {"$set": {"count": count, "updated_at": ts}},
        upsert=True,
    )

def update_footfall(db, direction: str):
    """Increment entry or exit counter."""
    field = "entries" if direction == "entry" else "exits"
    db.footfall.update_one(
        {"_id": "global"},
        {"$inc": {field: 1}},
        upsert=True,
    )

# ── Read helpers (sync, for tests) ────────────────────────────────────────────
def get_all_occupancy(db) -> dict:
    result = {}
    for doc in db.occupancy_cache.find():
        result[doc["zone"]] = doc["count"]
    return result
