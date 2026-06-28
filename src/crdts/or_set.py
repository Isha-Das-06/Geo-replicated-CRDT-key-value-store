import uuid
from typing import Any, Dict, Set, Tuple
from .base import CRDT


class ORSet(CRDT):
    """
    Observed-Remove Set CRDT.

    Each element is tagged with a unique token on add. Removes only remove specific
    tokens, not all tokens for an element. An element is present if any of its add-tokens
    are not in the remove set. This resolves the classic add/remove conflict: concurrent
    add and remove of the same element results in the element being present (add wins).

    State: { adds: Set[(element, token)], removes: Set[token] }
    Value: { element for (element, token) in adds if token not in removes }
    Merge: union of both add-sets and remove-sets
    """

    def __init__(
        self,
        node_id: str,
        adds: Set[Tuple[Any, str]] = None,
        removes: Set[str] = None,
    ):
        self.node_id = node_id
        self.adds: Set[Tuple[Any, str]] = adds or set()
        self.removes: Set[str] = removes or set()

    def add(self, element: Any) -> str:
        token = str(uuid.uuid4())
        self.adds.add((element, token))
        return token

    def remove(self, element: Any) -> None:
        tokens_to_remove = {token for (elem, token) in self.adds if elem == element}
        self.removes.update(tokens_to_remove)

    def contains(self, element: Any) -> bool:
        return any(
            token not in self.removes for (elem, token) in self.adds if elem == element
        )

    def value(self) -> Set[Any]:
        return {elem for (elem, token) in self.adds if token not in self.removes}

    def merge(self, other: "ORSet") -> "ORSet":
        merged_adds = self.adds | other.adds
        merged_removes = self.removes | other.removes
        return ORSet(self.node_id, merged_adds, merged_removes)

    def to_dict(self) -> Dict:
        return {
            "type": "ORSet",
            "node_id": self.node_id,
            "adds": list(self.adds),
            "removes": list(self.removes),
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "ORSet":
        adds = {(elem, token) for elem, token in data["adds"]}
        removes = set(data["removes"])
        return cls(data["node_id"], adds, removes)

    def __repr__(self) -> str:
        return f"ORSet(elements={self.value()})"
