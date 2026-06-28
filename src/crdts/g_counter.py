from typing import Dict
from .base import CRDT


class GCounter(CRDT):
    """
    Grow-only Counter CRDT.

    Each node maintains its own counter shard. The global value is the sum of all shards.
    Merge takes the element-wise maximum, guaranteeing convergence without coordination.

    State: { node_id -> count }
    Value: sum of all counts
    Merge: element-wise max
    """

    def __init__(self, node_id: str, counters: Dict[str, int] = None):
        self.node_id = node_id
        self.counters: Dict[str, int] = counters or {}

    def increment(self, amount: int = 1) -> None:
        self.counters[self.node_id] = self.counters.get(self.node_id, 0) + amount

    def value(self) -> int:
        return sum(self.counters.values())

    def merge(self, other: "GCounter") -> "GCounter":
        merged = dict(self.counters)
        for node, count in other.counters.items():
            merged[node] = max(merged.get(node, 0), count)
        return GCounter(self.node_id, merged)

    def to_dict(self) -> Dict:
        return {"type": "GCounter", "node_id": self.node_id, "counters": self.counters}

    @classmethod
    def from_dict(cls, data: Dict) -> "GCounter":
        return cls(data["node_id"], data["counters"])

    def __repr__(self) -> str:
        return f"GCounter(value={self.value()}, shards={self.counters})"
