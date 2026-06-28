from typing import Dict
from .base import CRDT
from .g_counter import GCounter


class PNCounter(CRDT):
    """
    Positive-Negative Counter CRDT.

    Composed of two GCounters: one for increments (P) and one for decrements (N).
    Value = P.value() - N.value(). Supports both increment and decrement.

    State: { P: GCounter, N: GCounter }
    Value: P - N
    Merge: merge P with P, merge N with N
    """

    def __init__(self, node_id: str, p: GCounter = None, n: GCounter = None):
        self.node_id = node_id
        self.p = p or GCounter(node_id)
        self.n = n or GCounter(node_id)

    def increment(self, amount: int = 1) -> None:
        self.p.increment(amount)

    def decrement(self, amount: int = 1) -> None:
        self.n.increment(amount)

    def value(self) -> int:
        return self.p.value() - self.n.value()

    def merge(self, other: "PNCounter") -> "PNCounter":
        merged_p = self.p.merge(other.p)
        merged_n = self.n.merge(other.n)
        return PNCounter(self.node_id, merged_p, merged_n)

    def to_dict(self) -> Dict:
        return {
            "type": "PNCounter",
            "node_id": self.node_id,
            "p": self.p.to_dict(),
            "n": self.n.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "PNCounter":
        p = GCounter.from_dict(data["p"])
        n = GCounter.from_dict(data["n"])
        return cls(data["node_id"], p, n)

    def __repr__(self) -> str:
        return f"PNCounter(value={self.value()}, P={self.p.counters}, N={self.n.counters})"
