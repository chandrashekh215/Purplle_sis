# Purplle Store Intelligence System (SIS) — MongoDB Edition

> **Zero Docker required.** Replaces Kafka + Redis + TimescaleDB with a single MongoDB instance.

---

## Architecture

```
CCTV / Synthetic Feed
        │
        ▼
  src/pipeline.py          ← OpenCV MOG2 motion detection, zone assignment
        │  writes directly
        ▼
   MongoDB (local)
   ├── events              ← all store events (time-indexed)
   ├── anomalies           ← detected anomaly records
   ├── zone_heatmap        ← per-zone motion percentages
   ├── occupancy_cache     ← live zone counts (replaces Redis HSET)
   └── footfall            ← running entry/exit counters
        │
        ▼
  api/main.py              ← FastAPI: REST + WebSocket + /metrics
        │
        ▼
  dashboard/index.html     ← Live HTML dashboard (WebSocket + polling)
```

### Why MongoDB instead of Kafka + Redis + TimescaleDB?

| Component     | Old stack            | New stack         | Why                                    |
|---------------|----------------------|-------------------|----------------------------------------|
| Message queue | Kafka                | Direct write      | No broker needed for single-node dev   |
| Cache         | Redis HSET           | MongoDB upsert    | `occupancy_cache` collection + index   |
| Time-series   | TimescaleDB          | MongoDB + indexes | TTL indexes achieve the same pruning   |
| Total services| 4 Docker containers  | **1 process**     | Laptop-friendly, no lag                |

MongoDB's `find + sort + limit` on indexed `timestamp` fields matches TimescaleDB query patterns for this scale. The `occupancy_cache` collection with upserts mirrors Redis HSET semantics exactly.

---

## Prerequisites

- Python 3.11+
- [MongoDB Community](https://www.mongodb.com/try/download/community) running locally on port 27017

```bash
# macOS
brew tap mongodb/brew && brew install mongodb-community && brew services start mongodb-community

# Ubuntu
sudo apt install -y mongodb && sudo systemctl start mongod

# Windows — install from mongodb.com installer, then:
net start MongoDB
```

---

## Quickstart

```bash
# 1. Clone / extract the project
cd purplle_sis_mongo

# 2. Create virtual environment
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Start the API server
uvicorn api.main:app --reload --port 8000

# 5. (Optional) Start the pipeline (synthetic events)
python -m src.pipeline

# 6. Open the dashboard
open http://localhost:8000/dashboard
# or visit http://localhost:8000/docs for Swagger UI
```

---

## Environment Variables

| Variable       | Default                          | Description                  |
|----------------|----------------------------------|------------------------------|
| `MONGO_URI`    | `mongodb://localhost:27017`      | MongoDB connection string    |
| `MONGO_DB`     | `purplle_sis`                    | Database name                |
| `PIPELINE_CAMERA_ID` | `CAM_1`                  | Camera ID for pipeline       |
| `PIPELINE_SOURCE`    | *(empty = synthetic)*    | Path to video file           |

---

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check + DB status |
| GET | `/metrics` | Prometheus metrics |
| GET | `/api/v1/store/occupancy` | Live zone occupancy |
| GET | `/api/v1/store/footfall?window=60m` | Entry/exit counts |
| GET | `/api/v1/zones/{zone_id}/dwell` | Dwell time percentiles |
| GET | `/api/v1/anomalies?severity=&limit=` | Anomaly feed |
| GET | `/api/v1/heatmap/{zone_id}` | Motion heatmap series |
| POST | `/api/v1/alerts/subscribe` | Webhook subscription |
| WS | `/ws/v1/live` | Live event stream |
| GET | `/dashboard` | Live HTML dashboard |
| GET | `/docs` | Swagger UI |

---

## Running Tests

```bash
pip install pytest pytest-asyncio httpx
pytest tests/ -v
```

---

## AI-Assisted Engineering Decisions

This system was designed with Claude (Anthropic) for several key decisions:

**1. MongoDB over Kafka + Redis + TimescaleDB**  
Prompted: *"What's the lightest possible stack that still handles real-time occupancy, time-series heatmaps, and anomaly writes?"*  
Decision: MongoDB's upsert semantics replace Redis cache; time-indexed collections replace TimescaleDB hypertables; direct writes replace Kafka for single-node dev.

**2. Anomaly threshold tuning**  
Prompted: *"What are reasonable defaults for queue length, dwell time, and tailgating detection in a mid-size beauty retail store?"*  
Defaults: queue > 4 people (critical), dwell > 120s (warning), 3 entries in < 4s (tailgating).

**3. Fallback-first API design**  
Prompted: *"How should endpoints behave when MongoDB is temporarily down?"*  
Decision: All endpoints return realistic sample data when DB is unavailable, enabling frontend dev without a running DB.

**4. Schema enforcement strategy**  
Prompted: *"Should Pydantic models be enforced at write time or read time?"*  
Decision: Pipeline uses dicts for speed; schema module provides Pydantic models for API validation layer — matching the challenge's partial-enforcement finding.

---

## Project Structure

```
purplle_sis_mongo/
├── api/
│   └── main.py              # FastAPI app (all endpoints)
├── src/
│   ├── db.py                # MongoDB layer (sync + async)
│   ├── pipeline.py          # Detection pipeline + event emitter
│   └── anomaly_engine.py    # Rules-based anomaly detection
├── schema/
│   └── events.py            # Pydantic event models
├── dashboard/
│   └── index.html           # Live store dashboard
├── tests/
│   └── test_api.py          # pytest test suite
├── requirements.txt
└── README.md
```
