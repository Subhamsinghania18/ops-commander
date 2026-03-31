# Real-World Input Connectors

## 1) External log files -> raw logs topic

Use the log feeder to publish real log lines:

- `python scripts/feed_logs_from_file.py --file C:/path/to/app.log --service payments-api --follow`

Supported formats:

- JSON logs with fields like message/level
- Common web access logs
- Plain text fallback

Output target:

- `events.raw.logs`

## 2) Prometheus metrics -> raw metrics topic

Use the Prometheus feeder:

- `python scripts/feed_metrics_from_prom.py --prom-url http://localhost:9090 --service payments-api`

You can pass custom PromQL mappings:

- `--query "cpu=avg(rate(process_cpu_seconds_total[1m]))"`
- `--query "error_rate=sum(rate(http_requests_total{status=~\"5..\"}[1m]))"`

Output target:

- `events.raw.metrics`

## 3) Alertmanager webhook -> raw alerts topic

POST Alertmanager payloads to:

- `POST /ingest/alerts/alertmanager`

Example:

```json
{
  "status": "firing",
  "labels": {
    "alertname": "HighErrorRate",
    "service": "payments-api",
    "severity": "critical"
  },
  "annotations": {
    "summary": "5xx rate exceeded 2%"
  },
  "fingerprint": "abc123"
}
```

Output target:

- `events.raw.alerts`

## Showcase artifact capture

Capture portfolio-ready live outputs from your running system:

- `./scripts/capture_showcase.ps1`

Bundle location:

- `artifacts/showcase/<timestamp>/`

## Distributed diagnosis demo injector

Use a deterministic multi-service chain to prove causal diagnosis quality:

- `python scripts/inject_distributed_incident.py --chain-id elite-chain-demo`

What this injects:

- root symptom on `orders-db`
- propagation to `orders-service`
- edge impact on `api-gateway`
- async backlog signal on `worker-service`
- mixed signal types (`metric`, `log`, `alert`) with shared correlation keys

Expected report characteristics:

- `affected_services` includes multiple services
- `how_failure_propagated` contains non-empty edges
- rankings include competing hypotheses (not a single item)
- `diagnosis.signal_coverage` contains all three signal types
