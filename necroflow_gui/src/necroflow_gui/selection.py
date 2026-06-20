from __future__ import annotations

from dataclasses import dataclass, field


SelectionKey = tuple[str, str]


@dataclass
class SelectionMemory:
    _selected: dict[SelectionKey, set[str]] = field(default_factory=dict)

    def has(self, pipeline_id: str, config_id: str) -> bool:
        return (pipeline_id, config_id) in self._selected

    def list(self, pipeline_id: str, config_id: str) -> set[str]:
        return set(self._selected.get((pipeline_id, config_id), set()))

    def toggle(self, pipeline_id: str, config_id: str, node_id: str) -> bool:
        key = (pipeline_id, config_id)
        selected = self._selected.setdefault(key, set())
        if node_id in selected:
            selected.remove(node_id)
            return False
        selected.add(node_id)
        return True

    def replace(self, pipeline_id: str, config_id: str, node_ids: set[str]) -> None:
        self._selected[(pipeline_id, config_id)] = set(node_ids)
