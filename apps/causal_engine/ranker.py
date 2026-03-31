from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import yaml

from apps.causal_engine.causal_graph import CausalGraph
from apps.causal_engine.dependency_graph import ServiceDependencyGraph


@dataclass(frozen=True)
class ScoringWeights:
    precedence: float
    dependency_centrality: float
    anomaly_magnitude: float
    blast_radius: float
    signal_quality_penalty: float

    @classmethod
    def from_config(cls, path: str | Path = "config/scoring.yaml") -> "ScoringWeights":
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        weights = raw.get("weights", {})
        return cls(
            precedence=float(weights.get("precedence", 0.30)),
            dependency_centrality=float(weights.get("dependency_centrality", 0.20)),
            anomaly_magnitude=float(weights.get("anomaly_magnitude", 0.25)),
            blast_radius=float(weights.get("blast_radius", 0.15)),
            signal_quality_penalty=float(weights.get("signal_quality_penalty", 0.10)),
        )


class RootCauseRanker:
    def __init__(self, dependency_graph: ServiceDependencyGraph, weights: ScoringWeights) -> None:
        self.dependency_graph = dependency_graph
        self.weights = weights

    def rank(self, graph: CausalGraph, top_k: int = 5) -> list[dict]:
        if not graph.service_evidence:
            return []

        services = list(graph.service_evidence.keys())
        earliest = min(ev.first_time for ev in graph.service_evidence.values())
        latest = max(ev.last_time for ev in graph.service_evidence.values())
        window_seconds = max(1.0, (latest - earliest).total_seconds())

        max_evidence_score = max(1.0, max(ev.score for ev in graph.service_evidence.values()))
        blast_map = self._blast_radius_map(graph)
        causal_support_map = self._causal_support_map(graph)

        scored: list[dict] = []
        for service in services:
            ev = graph.service_evidence[service]
            precedence_score = 1.0 - ((ev.first_time - earliest).total_seconds() / window_seconds)
            signal_diversity = self._signal_diversity(ev)
            causal_support = causal_support_map.get(service, 0.0)
            dependency_centrality = min(
                1.0,
                self._dependency_centrality(service) * 0.7 + causal_support * 0.3,
            )
            anomaly_magnitude = min(
                1.0,
                (ev.score / max_evidence_score) * 0.65 + signal_diversity * 0.35,
            )
            blast_radius = blast_map.get(service, 0.0)
            penalty = self._signal_quality_penalty(ev)

            raw_score = (
                self.weights.precedence * precedence_score
                + self.weights.dependency_centrality * dependency_centrality
                + self.weights.anomaly_magnitude * anomaly_magnitude
                + self.weights.blast_radius * blast_radius
                - self.weights.signal_quality_penalty * penalty
            )

            scored.append(
                {
                    "service": service,
                    "score": max(0.0, round(raw_score, 6)),
                    "component_scores": {
                        "precedence": round(precedence_score, 6),
                        "dependency_centrality": round(dependency_centrality, 6),
                        "anomaly_magnitude": round(anomaly_magnitude, 6),
                        "blast_radius": round(blast_radius, 6),
                        "signal_diversity": round(signal_diversity, 6),
                        "causal_support": round(causal_support, 6),
                        "signal_quality_penalty": round(penalty, 6),
                    },
                }
            )

        scored.sort(key=lambda x: x["score"], reverse=True)
        top = scored[:top_k]
        return self._attach_confidence(top)

    def _dependency_centrality(self, service: str) -> float:
        upstream = len(self.dependency_graph.upstream_of(service))
        downstream = len(self.dependency_graph.downstream_of(service))
        max_degree = max(1, max(len(v) for v in self.dependency_graph.dependencies.values()))
        return min(1.0, (upstream + downstream) / (2 * max_degree))

    def _blast_radius_map(self, graph: CausalGraph) -> dict[str, float]:
        service_set = set(graph.service_evidence.keys())
        edges_out: dict[str, set[str]] = {name: set() for name in service_set}
        weighted_out: dict[str, float] = {name: 0.0 for name in service_set}

        for edge in graph.edges:
            if edge.source_service in service_set and edge.target_service in service_set:
                edges_out[edge.source_service].add(edge.target_service)
                weighted_out[edge.source_service] += max(0.1, float(edge.weight))

        total = max(1, len(service_set) - 1)
        max_weighted = max(1.0, max(weighted_out.values()))
        result: dict[str, float] = {}
        for service, targets in edges_out.items():
            target_ratio = len(targets) / total
            weight_ratio = weighted_out.get(service, 0.0) / max_weighted
            result[service] = min(1.0, 0.6 * target_ratio + 0.4 * weight_ratio)
        return result

    def _causal_support_map(self, graph: CausalGraph) -> dict[str, float]:
        service_set = set(graph.service_evidence.keys())
        support: dict[str, float] = {name: 0.0 for name in service_set}
        for edge in graph.edges:
            if edge.source_service in service_set:
                support[edge.source_service] += max(0.1, float(edge.weight))
            if edge.target_service in service_set:
                support[edge.target_service] += max(0.05, float(edge.weight) * 0.6)

        max_support = max(1.0, max(support.values()))
        return {service: min(1.0, value / max_support) for service, value in support.items()}

    @staticmethod
    def _signal_diversity(ev) -> float:
        if not ev.signal_types:
            return 0.0
        return min(1.0, len(set(ev.signal_types)) / 3.0)

    @staticmethod
    def _signal_quality_penalty(ev) -> float:
        penalty = 0.0
        if ev.event_count <= 2:
            penalty += 0.35
        if ev.score < 3.0:
            penalty += 0.3
        if len(set(ev.signal_types)) <= 1:
            penalty += 0.35
        if not ev.critical_alert:
            penalty += 0.1
        return min(1.0, penalty)

    @staticmethod
    def _attach_confidence(rankings: list[dict]) -> list[dict]:
        if not rankings:
            return rankings

        max_score = max(item["score"] for item in rankings)
        temperature = 0.18
        exps = [math.exp((item["score"] - max_score) / temperature) for item in rankings]
        total = max(1e-9, sum(exps))
        probs = [x / total for x in exps]

        for idx, item in enumerate(rankings, start=1):
            current = item["score"]
            next_score = rankings[idx]["score"] if idx < len(rankings) else 0.0
            margin = max(0.0, current - next_score)
            comps = item["component_scores"]
            support = (
                comps["anomaly_magnitude"]
                + comps["signal_diversity"]
                + comps["causal_support"]
                + (1.0 - comps["signal_quality_penalty"])
            ) / 4.0

            confidence = min(
                0.99,
                max(0.05, probs[idx - 1] * 0.65 + support * 0.35 + min(0.15, margin * 0.4)),
            )
            item["confidence"] = round(confidence, 6)
            item["rank"] = idx
        return rankings
