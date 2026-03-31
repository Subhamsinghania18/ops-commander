# Architecture and Operations

## Runtime Topology

```mermaid
flowchart LR
  SIM[Simulator] --> RL[events.raw.logs]
  SIM --> RM[events.raw.metrics]
  SIM --> RA[events.raw.alerts]
  RL --> ING[Ingestor]
  RM --> ING
  RA --> ING
  ING --> NORM[events.normalized]
  NORM --> CORR[Correlator]
  CORR --> CAND[incidents.candidates]
  CAND --> CAUSAL[Causal Engine]
  CAUSAL --> DIAG[incidents.diagnosed]
  DIAG --> API[Reporter API]
  API --> REP[incidents.reports]
  API --> RJ[(replay_jobs)]
  RJ --> RW[Replay Worker]
  RW --> RL
  RW --> RM
  RW --> RA

  ING --> PG[(Postgres)]
  CAUSAL --> PG
  API --> PG
```

## Sequence for Live Diagnosis

```mermaid
sequenceDiagram
  participant S as Simulator
  participant I as Ingestor
  participant C as Correlator
  participant E as Causal Engine
  participant A as Reporter API

  S->>I: raw logs/metrics/alerts
  I->>C: normalized events
  C->>E: incident candidates
  E->>A: diagnosed incidents topic + DB snapshots
  A->>A: serve /incidents and /incidents/{id}/report
```

## Hardening Features

- Retry-backed produce/commit in stream consumers/producers
- Per-service runtime metrics snapshots (processed, dropped, errors, retries, avg latency)
- Kafka lag snapshots for consumer services
- Optional chaos drop-rate injection in simulator/consumers
- Replay and API operational endpoints for runtime introspection
