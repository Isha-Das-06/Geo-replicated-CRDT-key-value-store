from typing import Any, Dict, Optional
from .base import CRDT


class LWWRegister(CRDT):
    """
    Last-Write-Wins Register CRDT.

    Each write is tagged with a (timestamp, node_id) pair. On merge, the entry
    with the highest timestamp wins. Node ID breaks ties deterministically,
    preventing split-brain divergence from simultaneous writes.

    State: { value, timestamp, writer_id }
    Value: value with highest (timestamp, writer_id)
    Merge: pick entry with max (timestamp, writer_id)
    """

    def __init__(
        self,
        node_id: str,
        value: Any = None,
        timestamp: float = 0.0,
        writer_id: str = "",
    ):
        self.node_id = node_id
        self._value = value
        self.timestamp = timestamp
        self.writer_id = writer_id

    def write(self, value: Any, timestamp: float) -> None:
        if (timestamp, self.node_id) > (self.timestamp, self.writer_id):
            self._value = value
            self.timestamp = timestamp
            self.writer_id = self.node_id

    def value(self) -> Any:
        return self._value

    def merge(self, other: "LWWRegister") -> "LWWRegister":
        if (other.timestamp, other.writer_id) > (self.timestamp, self.writer_id):
            return LWWRegister(self.node_id, other._value, other.timestamp, other.writer_id)
        return LWWRegister(self.node_id, self._value, self.timestamp, self.writer_id)

    def to_dict(self) -> Dict:
        return {
            "type": "LWWRegister",
            "node_id": self.node_id,
            "value": self._value,
            "timestamp": self.timestamp,
            "writer_id": self.writer_id,
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "LWWRegister":
        return cls(data["node_id"], data["value"], data["timestamp"], data["writer_id"])

    def __repr__(self) -> str:
        return f"LWWRegister(value={self._value!r}, ts={self.timestamp:.3f}, writer={self.writer_id})"
