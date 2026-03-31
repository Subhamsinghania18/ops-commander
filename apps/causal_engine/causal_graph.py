from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from apps.causal_engine.dependency_graph import ServiceDependencyGraph


@dataclass(frozen=True)
class ServiceEvidence:
    service_name: str
    cluster_id: str
    score: float
    event_count: int
    first_time: datetime
    last_time: datetime
    critical_alert: bool
    signal_types: list[str]


@dataclass(frozen=True)
class CausalEdge:
    source_service: str
    target_service: str
    source_cluster: str
    target_cluster: str
    reason: str
    weight: float
    metadata: dict


@dataclass(frozen=True)
class CausalGraph:
    incident_id: str
    candidate_id: str
    window_start: datetime
    window_end: datetime
    affected_services: list[str]
    service_evidence: dict[str, ServiceEvidence]
    edges: list[CausalEdge]


class CausalGraphBuilder:
    def __init__(self, dependency_graph: ServiceDependencyGraph) -> None:
        self.dependency_graph = dependency_graph

    def build(self, candidate: dict, incident_id: str) -> CausalGraph:
        service_evidence = self._extract_service_evidence(candidate)
        provided_edges = self._extract_candidate_edges(candidate)
        inferred_edges = self._infer_dependency_edges(service_evidence)

        edge_map: dict[tuple[str, str, str, str], CausalEdge] = {}
        for edge in provided_edges + inferred_edges:
            key = (edge.source_service, edge.target_service, edge.source_cluster, edge.target_cluster)
            existing = edge_map.get(key)
            if existing is None or edge.weight > existing.weight:
                edge_map[key] = edge

        return CausalGraph(
            incident_id=incident_id,
            candidate_id=candidate["candidate_id"],
            window_start=datetime.fromisoformat(candidate["window_start"]),
            window_end=datetime.fromisoformat(candidate["window_end"]),
            affected_services=list(candidate["affected_services"]),
            service_evidence=service_evidence,
            edges=list(edge_map.values()),
        )

    def _extract_service_evidence(self, candidate: dict) -> dict[str, ServiceEvidence]:
        evidence_map: dict[str, ServiceEvidence] = {}
        for item in candidate.get("evidence", []):
            if not isinstance(item, dict):
                continue
            if "service_name" not in item:
                continue

            service = item["service_name"]
            cluster_id = item.get("cluster_id", f"cluster-{service}")
            window = item.get("window", {})
            first_time = datetime.fromisoformat(window.get("start", candidate["window_start"]))
            last_time = datetime.fromisoformat(window.get("end", candidate["window_end"]))
            signal_types = item.get("signal_types", [])
            evidence_map[service] = ServiceEvidence(
                service_name=service,
                cluster_id=cluster_id,
                score=float(item.get("score", 0.0)),
                event_count=int(item.get("event_count", 0)),
                first_time=first_time,
                last_time=last_time,
                critical_alert=bool(item.get("critical_alert", False)),
                signal_types=signal_types if isinstance(signal_types, list) else [],
            )
        return evidence_map

    def _extract_candidate_edges(self, candidate: dict) -> list[CausalEdge]:
        extracted: list[CausalEdge] = []
        for item in candidate.get("evidence", []):
            if not isinstance(item, dict):
                continue
            edge_summary = item.get("edge_summary")
            if not isinstance(edge_summary, list):
                continue
            for raw in edge_summary:
                metadata = raw.get("metadata", {})
                if not isinstance(metadata, dict):
                    metadata = {}
                extracted.append(
                    CausalEdge(
                        source_service=raw["source_service"],
                        target_service=raw["target_service"],
                        source_cluster=raw["source_cluster"],
                        target_cluster=raw["target_cluster"],
                        reason=raw.get("reason", "candidate-link"),
                        weight=float(raw.get("weight", 1.0)),
                        metadata={"origin": "candidate", **metadata},
                    )
                )
        return extracted

    def _infer_dependency_edges(self, service_evidence: dict[str, ServiceEvidence]) -> list[CausalEdge]:
        inferred: list[CausalEdge] = []
        services = list(service_evidence.keys())

        for source_service in services:
            for target_service in services:
                if source_service == target_service:
                    continue

                source = service_evidence[source_service]
                target = service_evidence[target_service]

                depth = self.dependency_graph.dependency_depth(target_service, source_service)
                if depth is None:
                    continue

                lag_seconds = (target.first_time - source.first_time).total_seconds()
                if lag_seconds < -10:
                    continue

                if lag_seconds > 180:
                    continue

                temporal_proximity = max(0.2, 1.0 - min(180.0, max(0.0, lag_seconds)) / 180.0)
                weight = round((1.0 / (depth + 1)) * temporal_proximity, 6)
                reason = "dependency-precedence" if lag_seconds >= 0 else "dependency-overlap"

                inferred.append(
                    CausalEdge(
                        source_service=source_service,
                        target_service=target_service,
                        source_cluster=source.cluster_id,
                        target_cluster=target.cluster_id,
                        reason=reason,
                        weight=weight,
                        metadata={
                            "dependency_depth": depth,
                            "lag_seconds": round(lag_seconds, 3),
                            "temporal_proximity": round(temporal_proximity, 6),
                            "origin": "inferred",
                        },
                    )
                )

        return inferred
