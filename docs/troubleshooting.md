# Troubleshooting Guide

## Core checks

1. Infrastructure up:
   - `docker compose ps`
2. API health:
   - `GET http://localhost:8000/health`
   - `GET http://localhost:8000/ops/readiness`
3. Runtime metrics:
   - `GET http://localhost:8000/ops/metrics`

## Common failures

1. Kafka produce/commit transient errors
- Symptoms: retry counters increasing in service logs.
- Action: verify Redpanda container health and broker connectivity.

2. High consumer lag
- Symptoms: lag in service metric snapshots for ingestor/correlator/causal-engine.
- Action: reduce simulator tick rate, scale service process instances, check CPU saturation.

3. Postgres outage
- Symptoms: reporter readiness degraded, ingestion/diagnosis write failures.
- Action: `docker compose start postgres`; services auto-recover via retry paths.

4. Replay job stuck queued
- Symptoms: replay_jobs status remains queued.
- Action: ensure replay worker process is running and archive path has JSONL replay data.

## Chaos drills

- Drop-message behavior: `./scripts/chaos_drill.ps1 -Scenario drop-messages`
- Consumer restart recovery:
  - `./scripts/chaos_drill.ps1 -Scenario restart-ingestor`
  - `./scripts/chaos_drill.ps1 -Scenario restart-correlator`
- DB outage/recovery:
  - `./scripts/chaos_drill.ps1 -Scenario db-outage -DurationSeconds 30`
  - `./scripts/chaos_drill.ps1 -Scenario db-recovery`
