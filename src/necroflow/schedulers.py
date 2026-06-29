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


class ConnectedComponentScheduler:
    """Prioritise nodes from the smallest connected component of remaining work.

    Components are computed once on the first call, then updated incrementally:
    when a node completes, only its component is re-BFS'd to detect splits.
    All other components are untouched — their sizes are O(1) lookups.
    """

    def __init__(self) -> None:
        self._adj: dict[str, list[str]] | None = None
        self._component_of: dict[str, int] = {}
        self._members: dict[int, set[str]] = {}
        self._sizes: dict[int, int] = {}
        self._prev_keys: set[str] = set()
        self._next_cid: int = 0

    def _new_cid(self) -> int:
        cid = self._next_cid
        self._next_cid += 1
        return cid

    def _build(self, nodes: list) -> None:
        keys = {n.key for n in nodes}
        adj: dict[str, list[str]] = {n.key: [] for n in nodes}
        for n in nodes:
            for p in n.parents:
                if p.key in keys:
                    adj[n.key].append(p.key)
                    adj[p.key].append(n.key)
        self._adj = adj

        visited: set[str] = set()
        for n in nodes:
            if n.key in visited:
                continue
            cid = self._new_cid()
            members: set[str] = set()
            frontier = [n.key]
            while frontier:
                k = frontier.pop()
                if k in visited:
                    continue
                visited.add(k)
                members.add(k)
                self._component_of[k] = cid
                frontier.extend(nb for nb in self._adj[k] if nb not in visited)
            self._members[cid] = members
            self._sizes[cid] = len(members)

    def _remove(self, key: str) -> None:
        cid = self._component_of.pop(key, None)
        if cid is None:
            return
        self._members[cid].discard(key)
        remaining_in_c = self._members[cid]
        if not remaining_in_c:
            del self._members[cid]
            del self._sizes[cid]
            return

        # Re-BFS within remaining_in_c to detect splits caused by removing key.
        unvisited = set(remaining_in_c)
        first = True
        while unvisited:
            start = next(iter(unvisited))
            sub: set[str] = set()
            frontier = [start]
            while frontier:
                k = frontier.pop()
                if k in sub:
                    continue
                sub.add(k)
                unvisited.discard(k)
                for nb in self._adj.get(k, []):
                    if nb in remaining_in_c and nb not in sub:
                        frontier.append(nb)
            if first:
                # Reuse the original component id for the first sub-component.
                self._members[cid] = sub
                self._sizes[cid] = len(sub)
                for k in sub:
                    self._component_of[k] = cid
                first = False
            else:
                new_cid = self._new_cid()
                self._members[new_cid] = sub
                self._sizes[new_cid] = len(sub)
                for k in sub:
                    self._component_of[k] = new_cid

    def __call__(self, ready: list, remaining: list) -> list:
        if self._adj is None:
            self._build(remaining)
            self._prev_keys = {n.key for n in remaining}
        else:
            current_keys = {n.key for n in remaining}
            for key in self._prev_keys - current_keys:
                self._remove(key)
            self._prev_keys = current_keys

        return sorted(ready, key=lambda n: self._sizes.get(self._component_of.get(n.key, -1), 0))


connected_component_scheduler = ConnectedComponentScheduler
