from __future__ import annotations

from sqlalchemy import delete, select

from libs.storage.db import RankingSnapshot, session_scope


class RankingRepository:
	def replace_rankings(self, incident_id: str, rankings: list[dict]) -> None:
		with session_scope() as session:
			session.execute(delete(RankingSnapshot).where(RankingSnapshot.incident_id == incident_id))
			for item in rankings:
				session.add(
					RankingSnapshot(
						incident_id=incident_id,
						rank=int(item["rank"]),
						service_name=item["service"],
						score=float(item["score"]),
						confidence=float(item["confidence"]),
						component_scores=item["component_scores"],
						explanation=item["explanation"],
					)
				)

	def list_rankings(self, incident_id: str) -> list[RankingSnapshot]:
		with session_scope() as session:
			stmt = (
				select(RankingSnapshot)
				.where(RankingSnapshot.incident_id == incident_id)
				.order_by(RankingSnapshot.rank.asc())
			)
			return list(session.execute(stmt).scalars().all())
