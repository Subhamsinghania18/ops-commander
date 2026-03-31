from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import timezone

from libs.common.ids import new_event_id
from libs.common.kafka import build_producer, ensure_topics, produce_json_with_retry
from libs.common.time import utc_now

COMMON_LOG_RE = re.compile(
    r"^(?P<ip>\S+)\s+\S+\s+\S+\s+\[(?P<ts>[^\]]+)\]\s+\"(?P<method>\S+)\s+(?P<path>[^\s]+)[^\"]*\"\s+(?P<status>\d{3})\s+(?P<size>\S+)"
)


def infer_severity(status: int) -> str:
    if status >= 500:
        return "error"
    if status >= 400:
        return "warn"
    return "info"


def parse_line(line: str, service_name: str, environment: str, region: str) -> dict | None:
    line = line.strip()
    if not line:
        return None

    try:
        raw = json.loads(line)
        message = raw.get("message") or raw.get("log") or line
        severity = str(raw.get("severity", raw.get("level", "info"))).lower()
        if severity not in {"debug", "info", "warn", "error", "critical"}:
            severity = "info"
        return {
            "event_id": new_event_id(),
            "signal_type": "log",
            "event_time": utc_now().astimezone(timezone.utc).isoformat(),
            "service_name": service_name,
            "instance_id": f"{service_name}-file-feed",
            "environment": environment,
            "region": region,
            "severity": severity,
            "correlation_keys": {"source": "file-log-feed"},
            "payload": {
                "message": str(message),
                "logger": str(raw.get("logger", "external")),
                "exception": raw.get("exception"),
                "code": raw.get("code"),
            },
        }
    except json.JSONDecodeError:
        pass

    m = COMMON_LOG_RE.match(line)
    if m:
        status = int(m.group("status"))
        return {
            "event_id": new_event_id(),
            "signal_type": "log",
            "event_time": utc_now().astimezone(timezone.utc).isoformat(),
            "service_name": service_name,
            "instance_id": f"{service_name}-file-feed",
            "environment": environment,
            "region": region,
            "severity": infer_severity(status),
            "correlation_keys": {
                "source": "file-log-feed",
                "http_method": m.group("method"),
                "http_path": m.group("path"),
                "status": str(status),
            },
            "payload": {
                "message": line,
                "logger": "access-log",
                "exception": None,
                "code": f"HTTP_{status}",
            },
        }

    return {
        "event_id": new_event_id(),
        "signal_type": "log",
        "event_time": utc_now().astimezone(timezone.utc).isoformat(),
        "service_name": service_name,
        "instance_id": f"{service_name}-file-feed",
        "environment": environment,
        "region": region,
        "severity": "info",
        "correlation_keys": {"source": "file-log-feed"},
        "payload": {
            "message": line,
            "logger": "plain-log",
            "exception": None,
            "code": None,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Feed external log file lines into raw log topic")
    parser.add_argument("--file", required=True, help="Path to log file")
    parser.add_argument("--service", default="external-app", help="Target service name")
    parser.add_argument("--topic", default=os.getenv("TOPIC_RAW_LOGS", "events.raw.logs"))
    parser.add_argument("--follow", action="store_true", help="Tail file continuously")
    parser.add_argument("--environment", default=os.getenv("ENVIRONMENT", "dev"))
    parser.add_argument("--region", default=os.getenv("REGION", "local"))
    args = parser.parse_args()

    ensure_topics([args.topic])
    producer = build_producer("external-log-feed")

    with open(args.file, "r", encoding="utf-8", errors="ignore") as fh:
        if args.follow:
            fh.seek(0, 2)

        while True:
            line = fh.readline()
            if not line:
                if not args.follow:
                    break
                time.sleep(0.2)
                continue

            event = parse_line(line, args.service, args.environment, args.region)
            if event is None:
                continue

            produce_json_with_retry(
                producer=producer,
                topic=args.topic,
                key=args.service,
                value=event,
            )
            print(json.dumps({"published": True, "service": args.service, "severity": event["severity"]}))

    producer.flush(5)


if __name__ == "__main__":
    main()
