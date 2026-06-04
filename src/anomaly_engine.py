"""Purplle SIS — Anomaly Detection Rules Engine"""
from dataclasses import dataclass, asdict
from collections import deque
from datetime import datetime, timezone
import uuid

THRESHOLDS = {
    "dwell_s":      120,   # seconds before prolonged dwell fires
    "queue_max":    4,     # people at billing before queue alert
    "entry_gap_s":  2.0,   # window for tailgating detection
}

@dataclass
class Anomaly:
    anomaly_id:   str
    camera_id:    str
    zone:         str
    anomaly_type: str
    severity:     str
    confidence:   float
    description:  str
    timestamp:    datetime
    data:         dict

    def to_dict(self) -> dict:
        d = asdict(self)
        d["_id"] = d["anomaly_id"]
        return d


class QueueMonitor:
    def check(self, queue_len: int, cam: str, ts: datetime) -> Anomaly | None:
        if queue_len > THRESHOLDS["queue_max"]:
            return Anomaly(
                anomaly_id=f"ANO_{uuid.uuid4().hex[:6].upper()}",
                camera_id=cam,
                zone="billing_counter",
                anomaly_type="queue_buildup",
                severity="critical",
                confidence=min(0.70 + queue_len * 0.04, 0.98),
                description=f"Queue length {queue_len} exceeds threshold {THRESHOLDS['queue_max']}",
                timestamp=ts,
                data={"queue_length": queue_len},
            )
        return None


class TailgatingDetector:
    def __init__(self):
        self._times: deque[datetime] = deque(maxlen=5)

    def record(self, cam: str, ts: datetime) -> Anomaly | None:
        self._times.append(ts)
        if len(self._times) >= 3:
            span = (list(self._times)[-1] - list(self._times)[-3]).total_seconds()
            if span < THRESHOLDS["entry_gap_s"] * 2:
                return Anomaly(
                    anomaly_id=f"ANO_{uuid.uuid4().hex[:6].upper()}",
                    camera_id=cam,
                    zone="store_entrance",
                    anomaly_type="tailgating",
                    severity="warning",
                    confidence=0.68,
                    description=f"3 entries detected in {span:.1f}s",
                    timestamp=ts,
                    data={"span_s": round(span, 2)},
                )
        return None


class DwellMonitor:
    """Fires if average dwell time in a zone exceeds threshold."""
    def check(self, zone: str, dwell_s: int, cam: str, ts: datetime) -> Anomaly | None:
        if dwell_s > THRESHOLDS["dwell_s"]:
            return Anomaly(
                anomaly_id=f"ANO_{uuid.uuid4().hex[:6].upper()}",
                camera_id=cam,
                zone=zone,
                anomaly_type="prolonged_dwell",
                severity="warning",
                confidence=0.75,
                description=f"Avg dwell {dwell_s}s > threshold {THRESHOLDS['dwell_s']}s in {zone}",
                timestamp=ts,
                data={"dwell_s": dwell_s},
            )
        return None


class AnomalyEngine:
    def __init__(self):
        self.queue  = QueueMonitor()
        self.tail   = TailgatingDetector()
        self.dwell  = DwellMonitor()

    def process(self, event: dict) -> list[Anomaly]:
        ts          = datetime.now(timezone.utc)
        event_type  = event.get("event_type")
        camera_id   = event.get("camera_id", "UNKNOWN")
        zone        = event.get("zone", "")
        data        = event.get("data", {})
        anomalies   = []

        if event_type == "occupancy_update":
            # Queue detection
            if zone == "billing_counter":
                a = self.queue.check(data.get("person_count", 0), camera_id, ts)
                if a:
                    anomalies.append(a)
            # Dwell detection
            dwell_s = data.get("dwell_time_avg_s")
            if dwell_s is not None:
                a = self.dwell.check(zone, dwell_s, camera_id, ts)
                if a:
                    anomalies.append(a)

        if event_type == "person_entry":
            a = self.tail.record(camera_id, ts)
            if a:
                anomalies.append(a)

        return anomalies
