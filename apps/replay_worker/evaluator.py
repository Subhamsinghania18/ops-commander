from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EvaluationSummary:
    incident_count: int
    top1_accuracy: float
    top3_accuracy: float
    mrr: float
    calibration_bins: list[dict]


class ReplayEvaluator:
    def evaluate(self, diagnosed_items: list[dict], expected_root_service: str | None) -> dict:
        per_incident: list[dict] = []
        if not diagnosed_items:
            summary = EvaluationSummary(
                incident_count=0,
                top1_accuracy=0.0,
                top3_accuracy=0.0,
                mrr=0.0,
                calibration_bins=[],
            )
            return {"summary": summary.__dict__, "per_incident": per_incident}

        top1_hits = 0
        top3_hits = 0
        rr_sum = 0.0

        confidence_points: list[tuple[float, int]] = []

        for item in diagnosed_items:
            rankings = item.get("rankings", [])
            ranked_services = [r.get("service") for r in rankings if isinstance(r, dict)]
            top_conf = float(rankings[0].get("confidence", 0.0)) if rankings else 0.0

            rank_pos = None
            if expected_root_service is not None:
                for idx, service in enumerate(ranked_services, start=1):
                    if service == expected_root_service:
                        rank_pos = idx
                        break

            top1_correct = rank_pos == 1 if rank_pos is not None else 0
            top3_correct = rank_pos is not None and rank_pos <= 3

            if top1_correct:
                top1_hits += 1
            if top3_correct:
                top3_hits += 1
            if rank_pos is not None and rank_pos > 0:
                rr_sum += 1.0 / rank_pos

            confidence_points.append((top_conf, int(bool(top1_correct))))

            per_incident.append(
                {
                    "incident_id": item.get("incident_id"),
                    "expected_root_service": expected_root_service,
                    "predicted_root_service": ranked_services[0] if ranked_services else None,
                    "rank_position": rank_pos,
                    "top1_correct": bool(top1_correct),
                    "top3_correct": bool(top3_correct),
                    "top_confidence": top_conf,
                }
            )

        n = len(diagnosed_items)
        summary = EvaluationSummary(
            incident_count=n,
            top1_accuracy=round(top1_hits / n, 6),
            top3_accuracy=round(top3_hits / n, 6),
            mrr=round(rr_sum / n, 6),
            calibration_bins=self._calibration(confidence_points),
        )
        return {"summary": summary.__dict__, "per_incident": per_incident}

    @staticmethod
    def _calibration(confidence_points: list[tuple[float, int]]) -> list[dict]:
        bins = [(0.0, 0.2), (0.2, 0.4), (0.4, 0.6), (0.6, 0.8), (0.8, 1.01)]
        out: list[dict] = []

        for low, high in bins:
            bucket = [(c, y) for c, y in confidence_points if low <= c < high]
            if not bucket:
                out.append(
                    {
                        "range": [low, min(high, 1.0)],
                        "count": 0,
                        "avg_confidence": 0.0,
                        "empirical_accuracy": 0.0,
                    }
                )
                continue

            count = len(bucket)
            avg_conf = sum(c for c, _ in bucket) / count
            accuracy = sum(y for _, y in bucket) / count
            out.append(
                {
                    "range": [low, min(high, 1.0)],
                    "count": count,
                    "avg_confidence": round(avg_conf, 6),
                    "empirical_accuracy": round(accuracy, 6),
                }
            )
        return out
