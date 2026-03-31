from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml


@dataclass(frozen=True)
class ServiceNode:
    name: str
    depends_on: set[str]


class ServiceDependencyGraph:
    def __init__(self, nodes: dict[str, ServiceNode]) -> None:
        self.nodes = nodes
        self.dependencies = {name: node.depends_on for name, node in nodes.items()}
        self.reverse_dependencies = self._build_reverse(self.dependencies)

    @classmethod
    def from_config(cls, path: str | Path = "config/services.yaml") -> "ServiceDependencyGraph":
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        nodes = {
            item["name"]: ServiceNode(name=item["name"], depends_on=set(item.get("depends_on", [])))
            for item in raw.get("services", [])
        }
        return cls(nodes)

    def upstream_of(self, service: str) -> set[str]:
        return set(self.dependencies.get(service, set()))

    def downstream_of(self, service: str) -> set[str]:
        return set(self.reverse_dependencies.get(service, set()))

    def dependency_depth(self, source: str, target: str, max_depth: int = 8) -> int | None:
        if source == target:
            return 0

        frontier = [(source, 0)]
        visited = {source}
        while frontier:
            node, depth = frontier.pop(0)
            if depth >= max_depth:
                continue
            for nxt in self.dependencies.get(node, set()):
                if nxt == target:
                    return depth + 1
                if nxt in visited:
                    continue
                visited.add(nxt)
                frontier.append((nxt, depth + 1))
        return None

    @staticmethod
    def _build_reverse(dependencies: dict[str, set[str]]) -> dict[str, set[str]]:
        reverse: dict[str, set[str]] = {name: set() for name in dependencies}
        for source, deps in dependencies.items():
            for dep in deps:
                reverse.setdefault(dep, set()).add(source)
        return reverse
