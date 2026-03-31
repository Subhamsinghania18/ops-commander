from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class ServiceNode:
    name: str
    tier: str
    depends_on: list[str]


def load_service_topology(config_path: str | Path = "config/services.yaml") -> dict[str, ServiceNode]:
    path = Path(config_path)
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    nodes: dict[str, ServiceNode] = {}
    for item in data["services"]:
        node = ServiceNode(
            name=item["name"],
            tier=item.get("tier", "core"),
            depends_on=list(item.get("depends_on", [])),
        )
        nodes[node.name] = node
    return nodes
