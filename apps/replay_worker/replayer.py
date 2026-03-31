from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime

from libs.common.kafka import produce_json_with_retry

from apps.replay_worker.reader import ReplayRecord


@dataclass(frozen=True)
class ReplayStats:
    total_records: int
    replay_start: str
    replay_end: str
    speed_factor: float


class StreamReplayer:
    def __init__(self, producer, speed_factor: float) -> None:
        self.producer = producer
        self.speed_factor = max(0.1, speed_factor)

    def replay(self, records: list[ReplayRecord]) -> ReplayStats:
        started_at = datetime.utcnow().isoformat() + "Z"

        previous_time = None
        for record in records:
            if previous_time is not None:
                gap_seconds = (record.event_time - previous_time).total_seconds()
                if gap_seconds > 0:
                    time.sleep(min(gap_seconds / self.speed_factor, 2.0))

            produce_json_with_retry(
                producer=self.producer,
                topic=record.topic,
                key=record.key,
                value=record.payload,
            )
            previous_time = record.event_time

        self.producer.flush(10)
        finished_at = datetime.utcnow().isoformat() + "Z"
        return ReplayStats(
            total_records=len(records),
            replay_start=started_at,
            replay_end=finished_at,
            speed_factor=self.speed_factor,
        )
