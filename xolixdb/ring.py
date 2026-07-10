"""
xolixdb.ring — Consistent hash ring with virtual nodes.

Chord-inspired placement (NANDA's own reference [6] is Chord): every
agent_id maps to an ordered preference list of nodes. RF distinct nodes
own each key; a "sloppy" tail of extra candidates provides availability
when replicas are down.
"""
from __future__ import annotations

import bisect
import hashlib


def _h(key: str) -> int:
    return int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], "big")


class HashRing:
    def __init__(self, vnodes: int = 64):
        self.vnodes = vnodes
        self._points: list[int] = []
        self._owners: dict[int, str] = {}
        self.nodes: set[str] = set()

    def add_node(self, node_id: str) -> None:
        if node_id in self.nodes:
            return
        self.nodes.add(node_id)
        for i in range(self.vnodes):
            p = _h(f"{node_id}#vn{i}")
            self._owners[p] = node_id
            bisect.insort(self._points, p)

    def remove_node(self, node_id: str) -> None:
        if node_id not in self.nodes:
            return
        self.nodes.discard(node_id)
        pts = [p for p, o in self._owners.items() if o == node_id]
        for p in pts:
            del self._owners[p]
            idx = bisect.bisect_left(self._points, p)
            if idx < len(self._points) and self._points[idx] == p:
                self._points.pop(idx)

    def preference_list(self, key: str, count: int) -> list[str]:
        """Walk clockwise from hash(key), collecting distinct nodes."""
        if not self._points:
            return []
        count = min(count, len(self.nodes))
        start = bisect.bisect(self._points, _h(key)) % len(self._points)
        seen: list[str] = []
        i = start
        while len(seen) < count:
            owner = self._owners[self._points[i]]
            if owner not in seen:
                seen.append(owner)
            i = (i + 1) % len(self._points)
            if i == start and len(seen) < count:
                break
        return seen
