from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

# Scheduler protocol:
#   scheduler(ready, remaining, available_resources) -> list[Node]
# ready     -- nodes whose parents are all done, not yet running
# remaining -- all not-yet-done, not-yet-running nodes (superset of ready)
# available_resources -- remaining capacity for capped resources
# Returns ready nodes in priority order; executor submits from the front.
Scheduler = Callable[["list", "list", "dict[str, int]"], "list"]


def fifo_scheduler(
    ready: list, remaining: list, available_resources: dict[str, int]
) -> list:
    """Submit ready nodes in topological (registration) order."""
    return ready


@dataclass
class _ConnectedComponentState:
    """Incremental component index owned by one scheduler closure."""

    adj: dict[Path, list[Path]] = field(default_factory=dict)
    component_of: dict[Path, int] = field(default_factory=dict)
    members: dict[int, set[Path]] = field(default_factory=dict)
    sizes: dict[int, int] = field(default_factory=dict)
    prev_keys: set[Path] | None = None
    next_cid: int = 0


def _new_component_id(state: _ConnectedComponentState) -> int:
    cid = state.next_cid
    state.next_cid += 1
    return cid


def _build_components(state: _ConnectedComponentState, nodes: list) -> None:
    keys = {node.relative_path for node in nodes}
    adj: dict[Path, list[Path]] = {node.relative_path: [] for node in nodes}
    for node in nodes:
        for parent in node.parents:
            if parent.relative_path in keys:
                adj[node.relative_path].append(parent.relative_path)
                adj[parent.relative_path].append(node.relative_path)
    state.adj = adj

    visited: set[Path] = set()
    for node in nodes:
        if node.relative_path in visited:
            continue
        cid = _new_component_id(state)
        members: set[Path] = set()
        frontier = [node.relative_path]
        while frontier:
            key = frontier.pop()
            if key in visited:
                continue
            visited.add(key)
            members.add(key)
            state.component_of[key] = cid
            frontier.extend(
                neighbour for neighbour in state.adj[key] if neighbour not in visited
            )
        state.members[cid] = members
        state.sizes[cid] = len(members)


def _remove_component_key(state: _ConnectedComponentState, key: Path) -> None:
    cid = state.component_of.pop(key)
    state.members[cid].remove(key)
    remaining_in_component = state.members[cid]
    if not remaining_in_component:
        del state.members[cid]
        del state.sizes[cid]
        return

    # Re-BFS only this component to detect splits caused by removing the key.
    unvisited = set(remaining_in_component)
    first = True
    while unvisited:
        start = next(iter(unvisited))
        subcomponent: set[Path] = set()
        frontier = [start]
        while frontier:
            candidate = frontier.pop()
            if candidate in subcomponent:
                continue
            subcomponent.add(candidate)
            unvisited.discard(candidate)
            for neighbour in state.adj[candidate]:
                if (
                    neighbour in remaining_in_component
                    and neighbour not in subcomponent
                ):
                    frontier.append(neighbour)
        if first:
            state.members[cid] = subcomponent
            state.sizes[cid] = len(subcomponent)
            for member in subcomponent:
                state.component_of[member] = cid
            first = False
        else:
            new_cid = _new_component_id(state)
            state.members[new_cid] = subcomponent
            state.sizes[new_cid] = len(subcomponent)
            for member in subcomponent:
                state.component_of[member] = new_cid


def _schedule_connected_components(
    state: _ConnectedComponentState,
    ready: list,
    remaining: list,
    available_resources: dict[str, int],
) -> list:
    current_keys = {node.relative_path for node in remaining}
    if state.prev_keys is None:
        _build_components(state, remaining)
    else:
        for key in state.prev_keys - current_keys:
            _remove_component_key(state, key)
    state.prev_keys = current_keys

    return sorted(
        ready,
        key=lambda node: state.sizes[state.component_of[node.relative_path]],
    )


def make_connected_component_scheduler() -> Scheduler:
    """Return an incremental smallest-component scheduler for one execution.

    The returned function builds its component index on its first call. Later
    calls re-BFS only components containing newly completed nodes. Create a new
    scheduler for each ``run()`` invocation so graph-specific state cannot
    leak between runs.
    """
    state = _ConnectedComponentState()

    def connected_component_scheduler(
        ready: list, remaining: list, available_resources: dict[str, int]
    ) -> list:
        return _schedule_connected_components(
            state, ready, remaining, available_resources
        )

    return connected_component_scheduler
