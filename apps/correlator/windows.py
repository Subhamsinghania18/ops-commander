from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from libs.common.models import EventEnvelope, EventType, Severity


def _severity_weight(severity: Severity) -> float:
    return {
        Severity.DEBUG: 0.2,
        Severity.INFO: 0.5,
        Severity.WARN: 1.2,
        Severity.ERROR: 2.0,
        Severity.CRITICAL: 3.0,
    }[severity]


def _event_type_weight(event_type: EventType) -> float:
    return {
        EventType.METRIC: 1.0,
        EventType.LOG: 1.4,
        EventType.ALERT: 2.2,
    }[event_type]


def _metric_anomaly_weight(metric_name: str, value: float) -> float:
    if metric_name == "error_rate":
        if value >= 0.04:
            return 1.8
        if value >= 0.02:
            return 1.4
        if value >= 0.01:
            return 1.0
        return 0.2

    if metric_name == "p95_latency_ms":
        if value >= 400:
            return 1.8
        if value >= 250:
            return 1.4
        if value >= 140:
            return 1.0
        return 0.2

    if metric_name == "queue_backlog":
        if value >= 120:
            return 1.6
        if value >= 60:
            return 1.2
        if value >= 25:
            return 0.8
        return 0.2

    if metric_name in {"cpu", "memory"}:
        if value >= 0.95:
            return 1.4
        if value >= 0.85:
            return 1.0
        return 0.4

    return 0.5


def _event_anomaly_weight(event: EventEnvelope) -> float:
    if event.event_type == EventType.ALERT:
        status = str(event.payload.get("status", "firing")).lower()
        return 1.8 if status == "firing" else 0.4

    if event.event_type == EventType.LOG:
        if event.payload.get("exception") or event.payload.get("code"):
            return 1.4
        return 0.35

    if event.event_type == EventType.METRIC:
        metric_name = str(event.payload.get("metric_name", ""))
        raw_value = event.payload.get("value", 0.0)
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            value = 0.0
        return _metric_anomaly_weight(metric_name, value)

    return 0.5


@dataclass
class ClusterEvidence:
    event_id: str
    event_time: datetime
    event_type: str
    severity: str
    correlation_keys: dict[str, str]


@dataclass
class SymptomCluster:
    cluster_id: str
    service_name: str
    first_event_time: datetime
    last_event_time: datetime
    event_count: int = 0
    score: float = 0.0
    signal_types: set[str] = field(default_factory=set)
    has_critical_alert: bool = False
    correlation_key_counts: dict[str, dict[str, int]] = field(default_factory=dict)
    evidence: list[ClusterEvidence] = field(default_factory=list)

    def add_event(self, event: EventEnvelope) -> None:
        event_time = event.event_time.astimezone(timezone.utc)
        self.last_event_time = max(self.last_event_time, event_time)
        self.first_event_time = min(self.first_event_time, event_time)
        self.event_count += 1
        self.signal_types.add(event.event_type.value)
        if event.event_type == EventType.ALERT and event.severity == Severity.CRITICAL:
            self.has_critical_alert = True

        self.score += (
            _severity_weight(event.severity)
            * _event_type_weight(event.event_type)
            * _event_anomaly_weight(event)
        )

        for key, value in event.correlation_keys.items():
            bucket = self.correlation_key_counts.setdefault(key, {})
            bucket[value] = bucket.get(value, 0) + 1

        if len(self.evidence) < 30:
            self.evidence.append(
                ClusterEvidence(
                    event_id=event.event_id,
                    event_time=event_time,
                    event_type=event.event_type.value,
                    severity=event.severity.value,
                    correlation_keys=event.correlation_keys,
                )
            )


class EventTimeWindowManager:
    def __init__(
        self,
        merge_gap_seconds: int = 15,
        close_after_seconds: int = 45,
        late_correction_grace_seconds: int = 40,
    ) -> None:
        self.merge_gap = timedelta(seconds=merge_gap_seconds)
        self.close_after = timedelta(seconds=close_after_seconds)
        self.late_correction_grace = timedelta(seconds=late_correction_grace_seconds)
        self._active: list[SymptomCluster] = []
        self._closed_recent: list[SymptomCluster] = []

    def ingest(self, event: EventEnvelope) -> SymptomCluster:
        event_time = event.event_time.astimezone(timezone.utc)
        target = self._pick_cluster(event.service_name, event_time)
        if target is None:
            target = SymptomCluster(
                cluster_id=str(uuid4()),
                service_name=event.service_name,
                first_event_time=event_time,
                last_event_time=event_time,
            )
            self._active.append(target)
        target.add_event(event)
        return target

    def ingest_late(self, event: EventEnvelope) -> SymptomCluster | None:
        event_time = event.event_time.astimezone(timezone.utc)
        candidates = [c for c in self._active + self._closed_recent if c.service_name == event.service_name]
        if not candidates:
            return None

        target = min(candidates, key=lambda c: abs((c.last_event_time - event_time).total_seconds()))
        if abs((target.last_event_time - event_time).total_seconds()) > self.late_correction_grace.total_seconds():
            return None
        target.add_event(event)
        return target

    def pop_finalized(self, watermark: datetime) -> list[SymptomCluster]:
        finalized: list[SymptomCluster] = []
        still_active: list[SymptomCluster] = []
        for cluster in self._active:
            if cluster.last_event_time + self.close_after <= watermark:
                finalized.append(cluster)
            else:
                still_active.append(cluster)
        self._active = still_active

        recent_cutoff = watermark - self.late_correction_grace
        self._closed_recent = [c for c in (self._closed_recent + finalized) if c.last_event_time >= recent_cutoff]
        return finalized

    def _pick_cluster(self, service_name: str, event_time: datetime) -> SymptomCluster | None:
        candidates = [c for c in self._active if c.service_name == service_name]
        if not candidates:
            return None

        candidates.sort(key=lambda c: c.last_event_time, reverse=True)
        for cluster in candidates:
            if event_time >= cluster.last_event_time - self.merge_gap and event_time <= cluster.last_event_time + self.merge_gap:
                return cluster
        return None
