from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class PropagationStep:
    service: str
    after_seconds: int
    effects: dict[str, float]


@dataclass(frozen=True)
class Scenario:
    name: str
    description: str
    start_after_seconds: int
    duration_seconds: int
    root_fault: dict
    propagation: list[PropagationStep]


def load_scenario(scenario_name: str, config_path: str | Path = "config/scenarios.yaml") -> Scenario:
    data = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    raw = data["scenarios"][scenario_name]
    return Scenario(
        name=scenario_name,
        description=raw["description"],
        start_after_seconds=raw["start_after_seconds"],
        duration_seconds=raw["duration_seconds"],
        root_fault=raw["root_fault"],
        propagation=[
            PropagationStep(
                service=s["service"],
                after_seconds=s["after_seconds"],
                effects=dict(s.get("effects", {})),
            )
            for s in raw.get("propagation", [])
        ],
    )


def active_effects(now_elapsed_seconds: int, scenario: Scenario) -> dict[str, dict[str, float]]:
    effects: dict[str, dict[str, float]] = {}
    if now_elapsed_seconds < scenario.start_after_seconds:
        return effects
    if now_elapsed_seconds > scenario.start_after_seconds + scenario.duration_seconds:
        return effects

    root_service = scenario.root_fault["service"]
    effects[root_service] = {
        "latency_multiplier": float(scenario.root_fault.get("latency_multiplier", 1.0)),
        "error_rate_multiplier": 1.5,
    }

    active_for = now_elapsed_seconds - scenario.start_after_seconds
    for step in scenario.propagation:
        if active_for >= step.after_seconds:
            effects[step.service] = dict(step.effects)

    return effects
