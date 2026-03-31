from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
import os
from pathlib import Path
from uuid import uuid4

import yaml

from apps.correlator.clustering import (
    ClusterSignalThresholds,
    candidate_time_span_ok,
    cluster_sort_key,
    is_noise_cluster,
    summarize_cluster,
)
from apps.correlator.windows import SymptomCluster


@dataclass(frozen=True)
class LinkConfig:
    max_temporal_gap_seconds: int = 45
    min_shared_key_matches: int = 1


class DependencyGraph:
    def __init__(self, dependencies: dict[str, set[str]]) -> None:
        self.dependencies = dependencies
        self.reverse_dependencies = self._reverse(dependencies)

    @classmethod
    def from_services_config(cls, path: str | Path = "config/services.yaml") -> "DependencyGraph":
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        dependencies = {
            item["name"]: set(item.get("depends_on", []))
            for item in data.get("services", [])
        }
        return cls(dependencies)

    def has_topology_proximity(self, a: str, b: str) -> bool:
        if a == b:
            return True
        if b in self.dependencies.get(a, set()):
            return True
        if a in self.dependencies.get(b, set()):
            return True
        if b in self.reverse_dependencies.get(a, set()):
            return True
        if a in self.reverse_dependencies.get(b, set()):
            return True
        return False

    @staticmethod
    def _reverse(dependencies: dict[str, set[str]]) -> dict[str, set[str]]:
        reverse: dict[str, set[str]] = {}
        for src, deps in dependencies.items():
            reverse.setdefault(src, set())
            for dep in deps:
                reverse.setdefault(dep, set()).add(src)
        return reverse


class CandidateLinker:
    def __init__(
        self,
        graph: DependencyGraph,
        link_config: LinkConfig | None = None,
        thresholds: ClusterSignalThresholds | None = None,
    ) -> None:
        self.graph = graph
        self.link_config = link_config or LinkConfig()
        self.thresholds = thresholds or ClusterSignalThresholds()
        keys = os.getenv("CORR_NON_LINKABLE_KEYS", "tick,scenario,environment,region")
        self.non_linkable_keys = {k.strip() for k in keys.split(",") if k.strip()}
        self._closed_clusters: list[SymptomCluster] = []
        self._emitted_signatures: set[str] = set()

    def ingest_finalized(self, clusters: list[SymptomCluster]) -> list[dict]:
        if not clusters:
            return []
        self._closed_clusters.extend(clusters)
        self._closed_clusters.sort(key=cluster_sort_key)

        filtered = [c for c in self._closed_clusters if not is_noise_cluster(c, self.thresholds)]
        groups = self._build_groups(filtered)
        candidate_groups = [g for g in groups if self._candidate_allowed(g)]
        candidates = [self._to_candidate(g) for g in candidate_groups]

        deduped: list[dict] = []
        for group, candidate in zip(candidate_groups, candidates, strict=False):
            signature = "|".join(sorted(c.cluster_id for c in group))
            if signature in self._emitted_signatures:
                continue
            self._emitted_signatures.add(signature)
            deduped.append(candidate)

        if self._closed_clusters:
            newest_time = max(c.last_event_time for c in self._closed_clusters)
            cutoff = newest_time - timedelta(minutes=10)
            self._closed_clusters = [c for c in self._closed_clusters if c.last_event_time >= cutoff]

        return deduped

    def _build_groups(self, clusters: list[SymptomCluster]) -> list[list[SymptomCluster]]:
        groups: list[list[SymptomCluster]] = []
        visited: set[str] = set()

        for seed in clusters:
            if seed.cluster_id in visited:
                continue
            stack = [seed]
            group: list[SymptomCluster] = []
            while stack:
                node = stack.pop()
                if node.cluster_id in visited:
                    continue
                visited.add(node.cluster_id)
                group.append(node)

                for candidate in clusters:
                    if candidate.cluster_id in visited:
                        continue
                    if self._linked(node, candidate):
                        stack.append(candidate)

            groups.append(group)

        return groups

    def _linked(self, a: SymptomCluster, b: SymptomCluster) -> bool:
        if a.cluster_id == b.cluster_id:
            return False

        temporal_gap = abs((a.last_event_time - b.first_event_time).total_seconds())
        if temporal_gap > self.link_config.max_temporal_gap_seconds:
            return False

        if not self.graph.has_topology_proximity(a.service_name, b.service_name):
            return False

        if not self._temporal_precedence_ok(a, b):
            return False

        if len(self._shared_key_matches(a, b)) < self.link_config.min_shared_key_matches:
            return False

        return True

    def _shared_key_matches(self, a: SymptomCluster, b: SymptomCluster) -> set[str]:
        matched: set[str] = set()
        key_space = (
            set(a.correlation_key_counts.keys())
            & set(b.correlation_key_counts.keys())
            - self.non_linkable_keys
        )
        for key in key_space:
            a_values = set(a.correlation_key_counts[key].keys())
            b_values = set(b.correlation_key_counts[key].keys())
            if a_values & b_values:
                matched.add(key)
        return matched

    def _orient_edge(
        self,
        a: SymptomCluster,
        b: SymptomCluster,
    ) -> tuple[SymptomCluster, SymptomCluster, str, int | None]:
        # dependencies[x] contains upstream services that x depends on.
        if b.service_name in self.graph.dependencies.get(a.service_name, set()):
            return (b, a, "dependency-upstream", 1)
        if a.service_name in self.graph.dependencies.get(b.service_name, set()):
            return (a, b, "dependency-upstream", 1)
        if b.service_name in self.graph.reverse_dependencies.get(a.service_name, set()):
            return (a, b, "dependency-upstream", 1)
        if a.service_name in self.graph.reverse_dependencies.get(b.service_name, set()):
            return (b, a, "dependency-upstream", 1)

        if a.last_event_time <= b.last_event_time:
            return (a, b, "temporal", None)
        return (b, a, "temporal", None)

    def _temporal_precedence_ok(self, a: SymptomCluster, b: SymptomCluster) -> bool:
        max_gap = timedelta(seconds=self.link_config.max_temporal_gap_seconds)

        # If a depends on b, b is upstream and should begin no later than a.
        if b.service_name in self.graph.dependencies.get(a.service_name, set()):
            return b.first_event_time <= a.last_event_time + max_gap

        # If b depends on a, a is upstream and should begin no later than b.
        if a.service_name in self.graph.dependencies.get(b.service_name, set()):
            return a.first_event_time <= b.last_event_time + max_gap

        return True

    def _candidate_allowed(self, group: list[SymptomCluster]) -> bool:
        if not group:
            return False
        if not candidate_time_span_ok(group, self.thresholds):
            return False

        affected_services = {c.service_name for c in group}
        if len(affected_services) >= self.thresholds.min_services_per_candidate:
            return True

        if len(group) == 1:
            c = group[0]
            return c.has_critical_alert and c.score >= self.thresholds.min_cluster_score

        return False

    def _to_candidate(self, group: list[SymptomCluster]) -> dict:
        group_sorted = sorted(group, key=cluster_sort_key)
        window_start = min(c.first_event_time for c in group_sorted)
        window_end = max(c.last_event_time for c in group_sorted)

        evidence = [summarize_cluster(c) for c in group_sorted]
        edges = self._build_edges(group_sorted)
        evidence.append(
            {
                "edge_summary": edges,
                "cluster_count": len(group_sorted),
                "total_score": round(sum(c.score for c in group_sorted), 3),
            }
        )

        return {
            "candidate_id": str(uuid4()),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "affected_services": sorted({c.service_name for c in group_sorted}),
            "evidence": evidence,
        }

    def _build_edges(self, clusters: list[SymptomCluster]) -> list[dict]:
        edges: list[dict] = []
        for i, a in enumerate(clusters):
            for b in clusters[i + 1 :]:
                if not self._linked(a, b):
                    continue

                shared_keys = sorted(self._shared_key_matches(a, b))
                temporal_gap_seconds = abs((a.last_event_time - b.first_event_time).total_seconds())

                source, target, relation, depth = self._orient_edge(a, b)
                reason = f"temporal+topology+correlation_keys[{','.join(shared_keys)}]"

                edges.append(
                    {
                        "source_cluster": source.cluster_id,
                        "target_cluster": target.cluster_id,
                        "source_service": source.service_name,
                        "target_service": target.service_name,
                        "reason": reason,
                        "metadata": {
                            "shared_keys": shared_keys,
                            "shared_key_count": len(shared_keys),
                            "temporal_gap_seconds": round(temporal_gap_seconds, 3),
                            "dependency_relation": relation,
                            "dependency_depth": depth,
                        },
                    }
                )
        return edges
