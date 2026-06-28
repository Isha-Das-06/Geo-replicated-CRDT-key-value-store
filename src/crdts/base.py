from abc import ABC, abstractmethod
from typing import Any, Dict


class CRDT(ABC):
    """Base class for all CRDTs. Every CRDT must support merge and serialization."""

    @abstractmethod
    def merge(self, other: "CRDT") -> "CRDT":
        """Merge another replica into this one. Must be commutative, associative, idempotent."""
        ...

    @abstractmethod
    def value(self) -> Any:
        """Return the current observed value."""
        ...

    @abstractmethod
    def to_dict(self) -> Dict:
        """Serialize state for network transmission."""
        ...

    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict) -> "CRDT":
        """Deserialize state received from a peer."""
        ...
