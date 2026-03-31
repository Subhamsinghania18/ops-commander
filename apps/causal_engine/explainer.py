from __future__ import annotations

from datetime import datetime, timezone

from apps.causal_engine.causal_graph import CausalGraph


class DiagnosisExplainer:
    def build_diagnosis(self, graph: CausalGraph, rankings: list[dict]) -> dict:
        if not rankings:
            return {
                "incident_id": graph.incident_id,
                "candidate_id": graph.candidate_id,
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "window_start": graph.window_start.isoformat(),
                "window_end": graph.window_end.isoformat(),
                "affected_services": graph.affected_services,
                "rankings": [],
                "competing_hypotheses": [],
                "causal_edges": [],
                "summary": {
                    "root_cause": None,
                    "confidence": 0.0,
                    "why": "insufficient evidence",
                },
            }

        top = rankings[0]
        competitors = rankings[1:4]

        enriched_rankings: list[dict] = []
        for item in rankings:
            service = item["service"]
            ev = graph.service_evidence[service]
            explanation = {
                "service": service,
                "evidence": {
                    "cluster_id": ev.cluster_id,
                    "event_count": ev.event_count,
                    "critical_alert": ev.critical_alert,
                    "signal_types": ev.signal_types,
                    "first_seen": ev.first_time.isoformat(),
                    "last_seen": ev.last_time.isoformat(),
                },
                "scores": item["component_scores"],
                "causal_support": self._service_edges(graph, service),
            }
            enriched_rankings.append(
                {
                    "rank": item["rank"],
                    "service": service,
                    "score": item["score"],
                    "confidence": item["confidence"],
                    "component_scores": item["component_scores"],
                    "explanation": explanation,
                }
            )

        return {
            "incident_id": graph.incident_id,
            "candidate_id": graph.candidate_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "window_start": graph.window_start.isoformat(),
            "window_end": graph.window_end.isoformat(),
            "affected_services": graph.affected_services,
            "rankings": enriched_rankings,
            "competing_hypotheses": [
                {
                    "service": c["service"],
                    "score": c["score"],
                    "confidence": c["confidence"],
                }
                for c in competitors
            ],
            "causal_edges": [
                {
                    "source_service": e.source_service,
                    "target_service": e.target_service,
                    "source_cluster": e.source_cluster,
                    "target_cluster": e.target_cluster,
                    "reason": e.reason,
                    "weight": e.weight,
                    "metadata": e.metadata,
                }
                for e in graph.edges
            ],
            "summary": {
                "root_cause": top["service"],
                "confidence": top["confidence"],
                "why": self._summary_reason(top),
            },
        }

    def _service_edges(self, graph: CausalGraph, service: str) -> dict:
        incoming = []
        outgoing = []
        for edge in graph.edges:
            if edge.source_service == service:
                outgoing.append(
                    {
                        "target_service": edge.target_service,
                        "reason": edge.reason,
                        "weight": edge.weight,
                        "metadata": edge.metadata,
                    }
                )
            if edge.target_service == service:
                incoming.append(
                    {
                        "source_service": edge.source_service,
                        "reason": edge.reason,
                        "weight": edge.weight,
                        "metadata": edge.metadata,
                    }
                )
        return {"incoming": incoming, "outgoing": outgoing}

    @staticmethod
    def _summary_reason(top: dict) -> str:
        c = top["component_scores"]
        return (
            "highest weighted evidence from precedence, dependency centrality, "
            f"and anomaly magnitude (p={c['precedence']}, d={c['dependency_centrality']}, a={c['anomaly_magnitude']})"
        )
