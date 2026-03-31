from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from apps.correlator.windows import SymptomCluster


@dataclass(frozen=True)
class ClusterSignalThresholds:
    min_cluster_score: float = 5.5
    min_events_per_cluster: int = 3
    min_signal_types_per_cluster: int = 2
    min_services_per_candidate: int = 2
    max_candidate_span_seconds: int = 120


def is_noise_cluster(cluster: SymptomCluster, thresholds: ClusterSignalThresholds) -> bool:
    if cluster.has_critical_alert:
        return False
    if (
        cluster.score >= thresholds.min_cluster_score
        and cluster.event_count >= thresholds.min_events_per_cluster
        and len(cluster.signal_types) >= thresholds.min_signal_types_per_cluster
    ):
        return False
    if (
        cluster.score >= thresholds.min_cluster_score * 1.4
        and cluster.event_count >= thresholds.min_events_per_cluster * 2
    ):
        return False
    return True


def candidate_time_span_ok(clusters: list[SymptomCluster], thresholds: ClusterSignalThresholds) -> bool:
    if not clusters:
        return False
    start = min(c.first_event_time for c in clusters)
    end = max(c.last_event_time for c in clusters)
    return end - start <= timedelta(seconds=thresholds.max_candidate_span_seconds)


def summarize_cluster(cluster: SymptomCluster) -> dict:
    return {
        "cluster_id": cluster.cluster_id,
        "service_name": cluster.service_name,
        "event_count": cluster.event_count,
        "score": round(cluster.score, 3),
        "window": {
            "start": cluster.first_event_time.isoformat(),
            "end": cluster.last_event_time.isoformat(),
        },
        "signal_types": sorted(cluster.signal_types),
        "critical_alert": cluster.has_critical_alert,
        "top_correlation_keys": _top_correlation_keys(cluster),
        "sample_event_ids": [e.event_id for e in cluster.evidence[:5]],
    }


def _top_correlation_keys(cluster: SymptomCluster) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value_counts in cluster.correlation_key_counts.items():
        best_value = max(value_counts.items(), key=lambda x: x[1])[0]
        result[key] = best_value
    return result


def cluster_sort_key(cluster: SymptomCluster) -> tuple[datetime, float]:
    return (cluster.first_event_time, -cluster.score)
