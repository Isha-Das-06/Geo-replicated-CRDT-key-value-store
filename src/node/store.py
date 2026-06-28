"""
Per-node CRDT key-value store.

Each key maps to one of four CRDT types. Writes always succeed locally
(AP guarantee from CAP theorem). Convergence happens through gossip.
"""

import threading
import time
from typing import Any, Dict, Optional
from src.crdts import GCounter, PNCounter, LWWRegister, ORSet


CRDT_TYPES = {
    "gcounter": GCounter,
    "pncounter": PNCounter,
    "lwwregister": LWWRegister,
    "orset": ORSet,
}


class CRDTStore:
    def __init__(self, node_id: str):
        self.node_id = node_id
        self._store: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._write_count = 0
        self._merge_count = 0

    # ------------------------------------------------------------------
    # Write operations (always local, always succeed)
    # ------------------------------------------------------------------

    def create(self, key: str, crdt_type: str) -> None:
        with self._lock:
            if key not in self._store:
                cls = CRDT_TYPES[crdt_type.lower()]
                self._store[key] = cls(self.node_id)

    def increment(self, key: str, amount: int = 1) -> bool:
        with self._lock:
            crdt = self._store.get(key)
            if crdt is None or not hasattr(crdt, "increment"):
                return False
            crdt.increment(amount)
            self._write_count += 1
            return True

    def decrement(self, key: str, amount: int = 1) -> bool:
        with self._lock:
            crdt = self._store.get(key)
            if crdt is None or not hasattr(crdt, "decrement"):
                return False
            crdt.decrement(amount)
            self._write_count += 1
            return True

    def write(self, key: str, value: Any, timestamp: Optional[float] = None) -> bool:
        with self._lock:
            crdt = self._store.get(key)
            if crdt is None or not hasattr(crdt, "write"):
                return False
            crdt.write(value, timestamp or time.time())
            self._write_count += 1
            return True

    def add(self, key: str, element: Any) -> bool:
        with self._lock:
            crdt = self._store.get(key)
            if crdt is None or not hasattr(crdt, "add"):
                return False
            crdt.add(element)
            self._write_count += 1
            return True

    def remove(self, key: str, element: Any) -> bool:
        with self._lock:
            crdt = self._store.get(key)
            if crdt is None or not hasattr(crdt, "remove"):
                return False
            crdt.remove(element)
            self._write_count += 1
            return True

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def read(self, key: str) -> Optional[Any]:
        with self._lock:
            crdt = self._store.get(key)
            return crdt.value() if crdt else None

    def get_crdt(self, key: str):
        with self._lock:
            return self._store.get(key)

    def keys(self):
        with self._lock:
            return list(self._store.keys())

    # ------------------------------------------------------------------
    # Anti-entropy merge (called by gossip on receiving peer state)
    # ------------------------------------------------------------------

    def merge_remote(self, key: str, remote_dict: dict) -> bool:
        """Merge a serialized CRDT state from a peer. Idempotent and commutative."""
        with self._lock:
            crdt_type = remote_dict.get("type", "").lower()
            cls = CRDT_TYPES.get(crdt_type)
            if cls is None:
                return False

            remote_crdt = cls.from_dict(remote_dict)

            if key not in self._store:
                self._store[key] = remote_crdt
            else:
                self._store[key] = self._store[key].merge(remote_crdt)

            self._merge_count += 1
            return True

    def full_state(self) -> Dict[str, dict]:
        """Serialize entire store for full-state gossip."""
        with self._lock:
            return {key: crdt.to_dict() for key, crdt in self._store.items()}

    def stats(self) -> dict:
        with self._lock:
            return {
                "node_id": self.node_id,
                "keys": len(self._store),
                "writes": self._write_count,
                "merges": self._merge_count,
            }
