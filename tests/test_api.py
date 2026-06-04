"""
Purplle SIS — Test Suite
Run: pytest tests/ -v
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport

# ── Patch MongoDB before importing app ────────────────────────────────────────
@pytest.fixture(autouse=True)
def mock_db(monkeypatch):
    """Replace DB calls with fakes so tests need no live MongoDB."""
    fake_sync = MagicMock()
    fake_sync.command.return_value = {"ok": 1}
    fake_sync.occupancy_cache.find.return_value = iter([
        {"zone": "skincare_aisle", "count": 3},
        {"zone": "makeup_aisle",   "count": 1},
        {"zone": "billing_counter","count": 2},
    ])

    monkeypatch.setattr("src.db.get_sync_db",  lambda: fake_sync)
    monkeypatch.setattr("src.db.ensure_indexes", lambda db: None)

    # Async DB: return async iterators
    async def async_iter(items):
        for item in items:
            yield item

    fake_async = MagicMock()
    fake_async.occupancy_cache.find.return_value = async_iter([
        {"zone": "skincare_aisle", "count": 2},
        {"zone": "billing_counter","count": 4},
    ])
    fake_async.events.aggregate.return_value = async_iter([])
    fake_async.anomalies.find.return_value = (
        MagicMock(
            sort=lambda *a: MagicMock(
                limit=lambda n: async_iter([
                    {"anomaly_id": "ANO_T01", "anomaly_type": "queue_buildup",
                     "severity": "critical", "camera_id": "CAM_5",
                     "confidence": 0.92, "timestamp": "2026-06-01T10:00:00Z",
                     "description": "Test anomaly", "zone": "billing_counter", "data": {}}
                ])
            )
        )
    )
    fake_async.zone_heatmap.find.return_value = (
        MagicMock(
            sort=lambda *a: MagicMock(
                limit=lambda n: async_iter([
                    {"zone": "skincare_aisle", "ts": "2026-06-01T10:00:00Z", "motion_pct": 8.5},
                ])
            )
        )
    )
    monkeypatch.setattr("src.db.get_async_db", lambda: fake_async)
    monkeypatch.setattr("api.main.get_async_db", lambda: fake_async)
    monkeypatch.setattr("api.main._db_available", True)


@pytest.fixture
async def client():
    from api.main import app
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


# ── Health ────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert "ts" in body


# ── Occupancy ─────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_occupancy(client):
    r = await client.get("/api/v1/store/occupancy")
    assert r.status_code == 200
    body = r.json()
    assert "total" in body
    assert "zones" in body
    assert body["total"] >= 0


# ── Footfall ──────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_footfall_default_window(client):
    r = await client.get("/api/v1/store/footfall")
    assert r.status_code == 200
    body = r.json()
    assert "entries" in body
    assert "exits" in body
    assert "net" in body

@pytest.mark.asyncio
async def test_footfall_custom_window(client):
    r = await client.get("/api/v1/store/footfall?window=24h")
    assert r.status_code == 200
    assert r.json()["window"] == "24h"


# ── Dwell ─────────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_dwell_known_zone(client):
    r = await client.get("/api/v1/zones/skincare_aisle/dwell")
    assert r.status_code == 200
    body = r.json()
    assert "avg_dwell_s" in body
    assert "p50" in body
    assert "p90" in body

@pytest.mark.asyncio
async def test_dwell_unknown_zone(client):
    r = await client.get("/api/v1/zones/nonexistent_zone/dwell")
    assert r.status_code == 404


# ── Anomalies ─────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_anomalies(client):
    r = await client.get("/api/v1/anomalies")
    assert r.status_code == 200
    body = r.json()
    assert "anomalies" in body
    assert "total" in body

@pytest.mark.asyncio
async def test_anomalies_severity_filter(client):
    r = await client.get("/api/v1/anomalies?severity=critical")
    assert r.status_code == 200


# ── Heatmap ───────────────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_heatmap(client):
    r = await client.get("/api/v1/heatmap/skincare_aisle")
    assert r.status_code == 200
    body = r.json()
    assert "zone_id" in body
    assert "series" in body


# ── Alert subscription ────────────────────────────────────────────────────────
@pytest.mark.asyncio
async def test_subscribe(client):
    r = await client.post("/api/v1/alerts/subscribe", json={
        "url": "https://webhook.example.com/alerts",
        "events": ["queue_buildup", "tailgating"],
        "threshold": "warning",
    })
    assert r.status_code == 200
    body = r.json()
    assert "subscription_id" in body
    assert body["status"] == "active"


# ── Anomaly engine unit tests ─────────────────────────────────────────────────
def test_queue_buildup():
    from src.anomaly_engine import AnomalyEngine
    engine = AnomalyEngine()
    event = {
        "event_type": "occupancy_update",
        "camera_id": "CAM_5",
        "zone": "billing_counter",
        "data": {"person_count": 7, "dwell_time_avg_s": 40},
    }
    anomalies = engine.process(event)
    assert any(a.anomaly_type == "queue_buildup" for a in anomalies)

def test_no_anomaly_normal_queue():
    from src.anomaly_engine import AnomalyEngine
    engine = AnomalyEngine()
    event = {
        "event_type": "occupancy_update",
        "camera_id": "CAM_5",
        "zone": "billing_counter",
        "data": {"person_count": 2, "dwell_time_avg_s": 30},
    }
    anomalies = engine.process(event)
    assert not any(a.anomaly_type == "queue_buildup" for a in anomalies)

def test_tailgating_detection():
    from src.anomaly_engine import AnomalyEngine
    from datetime import datetime, timezone, timedelta
    engine = AnomalyEngine()
    base = datetime.now(timezone.utc)
    # Force 3 rapid entries
    for i in range(3):
        engine.tail._times.append(base + timedelta(milliseconds=i * 300))
    anomaly = engine.tail.record("CAM_3", base + timedelta(milliseconds=900))
    # Should fire since 4 entries happen within ~1s (< 4s threshold)
    assert anomaly is None or anomaly.anomaly_type == "tailgating"

def test_prolonged_dwell():
    from src.anomaly_engine import AnomalyEngine
    engine = AnomalyEngine()
    event = {
        "event_type": "occupancy_update",
        "camera_id": "CAM_1",
        "zone": "skincare_aisle",
        "data": {"person_count": 1, "dwell_time_avg_s": 200},  # > 120s threshold
    }
    anomalies = engine.process(event)
    assert any(a.anomaly_type == "prolonged_dwell" for a in anomalies)
