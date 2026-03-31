from __future__ import annotations

from libs.storage.db import QuarantineEvent, session_scope


def save_quarantine_event(source_topic: str, reason: str, payload: dict) -> None:
    with session_scope() as session:
        session.add(QuarantineEvent(source_topic=source_topic, reason=reason[:512], payload=payload))
