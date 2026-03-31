from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class ReplayRecord:
    topic: str
    key: str
    payload: dict
    event_time: datetime


class ArchiveReader:
    def __init__(self, archive_root: str | Path) -> None:
        self.archive_root = Path(archive_root)

    def read_records(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        scenario_name: str | None = None,
    ) -> list[ReplayRecord]:
        files = sorted(self.archive_root.rglob("*.jsonl"))
        records: list[ReplayRecord] = []

        for file_path in files:
            for line in file_path.read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                raw = json.loads(line)
                parsed = self._to_record(raw)
                if parsed is None:
                    continue
                if start_time and parsed.event_time < start_time:
                    continue
                if end_time and parsed.event_time > end_time:
                    continue
                if scenario_name and parsed.payload.get("correlation_keys", {}).get("scenario") != scenario_name:
                    continue
                records.append(parsed)

        records.sort(key=lambda r: (r.event_time, r.topic, r.key))
        return records

    @staticmethod
    def extract_scenario_distribution(records: list[ReplayRecord]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for record in records:
            scenario = record.payload.get("correlation_keys", {}).get("scenario")
            if not scenario:
                continue
            counts[scenario] = counts.get(scenario, 0) + 1
        return counts

    @staticmethod
    def _to_record(raw: dict) -> ReplayRecord | None:
        topic = raw.get("topic")
        payload = raw.get("value")

        if not isinstance(topic, str) or not isinstance(payload, dict):
            return None

        key = str(raw.get("key", payload.get("service_name", "unknown")))

        if isinstance(raw.get("event_time"), str):
            event_time = datetime.fromisoformat(raw["event_time"])
        elif isinstance(payload.get("event_time"), str):
            event_time = datetime.fromisoformat(payload["event_time"])
        elif isinstance(payload.get("ingest_time"), str):
            event_time = datetime.fromisoformat(payload["ingest_time"])
        else:
            return None

        return ReplayRecord(topic=topic, key=key, payload=payload, event_time=event_time)
