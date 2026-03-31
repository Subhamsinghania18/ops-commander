from __future__ import annotations

from sqlalchemy import delete, select

from libs.storage.db import EvidenceEdge, session_scope


class EvidenceRepository:
    def replace_edges(self, incident_id: str, edges: list[dict]) -> None:
        with session_scope() as session:
            session.execute(delete(EvidenceEdge).where(EvidenceEdge.incident_id == incident_id))
            for edge in edges:
                session.add(
                    EvidenceEdge(
                        incident_id=incident_id,
                        source_service=edge["source_service"],
                        target_service=edge["target_service"],
                        source_cluster=edge["source_cluster"],
                        target_cluster=edge["target_cluster"],
                        reason=edge.get("reason", "unknown"),
                        weight=float(edge.get("weight", 1.0)),
                        edge_metadata=edge.get("metadata", {}),
                    )
                )

    def list_edges(self, incident_id: str) -> list[EvidenceEdge]:
        with session_scope() as session:
            stmt = select(EvidenceEdge).where(EvidenceEdge.incident_id == incident_id)
            return list(session.execute(stmt).scalars().all())
