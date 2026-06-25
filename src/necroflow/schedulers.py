from __future__ import annotations

from typing import Callable

from necroflow.nodes import iter_connected_components

# Scheduler protocol:
#   scheduler(ready, remaining) -> list[Node]
# ready     -- nodes whose parents are all done, not yet running
# remaining -- all not-yet-done, not-yet-running nodes (superset of ready)
# Returns ready nodes in priority order; executor submits from the front.
Scheduler = Callable[["list", "list"], "list"]


def fifo_scheduler(ready: list, remaining: list) -> list:
    """Submit ready nodes in topological (registration) order."""
    return ready


def connected_component_scheduler(ready: list, remaining: list) -> list:
    """Prioritise nodes from the smallest connected component of remaining work."""
    node_to_size: dict[str, int] = {}
    for component in iter_connected_components(remaining):
        size = len(component)
        for n in component:
            node_to_size[n.key] = size
    return sorted(ready, key=lambda n: node_to_size.get(n.key, 0))
