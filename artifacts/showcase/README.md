# Showcase Artifacts

This folder stores portfolio-ready outputs captured from live runs of Ops Commander.

## Included sample

- `sample_distributed_incident/`

This sample demonstrates:

- multi-service incident detection
- non-empty failure propagation edges
- ranked competing root-cause hypotheses
- mixed evidence signals (`metric`, `log`, `alert`)

## Regenerating a fresh bundle

1. Start platform services and app processes.
2. Inject a deterministic distributed scenario:
   - `python scripts/inject_distributed_incident.py --chain-id elite-chain-demo`
3. Capture artifacts:
   - `./scripts/capture_showcase.ps1 -WaitSeconds 45`

By default, new runtime bundles are ignored by `.gitignore` so the repository stays clean.
