"""
Region node — simulates a single datacenter.

Each node:
  1. Maintains a local CRDTStore (writes always succeed — AP behavior)
  2. Runs periodic anti-entropy gossip to push state to peers
  3. Receives incoming gossip and merges state

The gossip protocol uses full-state exchange (simpler than delta-state for
this demo). In production you'd use delta-CRDTs to reduce bandwidth.
"""

import threading
import time
import logging
import random
from typing import Dict, List, Optional, Callable

from src.node.store import CRDTStore
from src.network.simulator import NetworkSimulator

logger = logging.getLogger(__name__)


class RegionNode:
    def __init__(
        self,
        node_id: str,
        region_name: str,
        network: NetworkSimulator,
        gossip_interval_s: float = 1.0,
    ):
        self.node_id = node_id
        self.region_name = region_name
        self.network = network
        self.gossip_interval_s = gossip_interval_s

        self.store = CRDTStore(node_id)
        self._peers: List["RegionNode"] = []
        self._running = False
        self._gossip_thread: Optional[threading.Thread] = None

        # Metrics
        self._write_attempts = 0
        self._write_successes = 0
        self._gossip_sent = 0
        self._gossip_received = 0
        self._convergence_callbacks: List[Callable] = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def add_peer(self, peer: "RegionNode") -> None:
        if peer not in self._peers and peer is not self:
            self._peers.append(peer)

    def start(self) -> None:
        self._running = True
        self._gossip_thread = threading.Thread(
            target=self._gossip_loop, name=f"gossip-{self.node_id}", daemon=True
        )
        self._gossip_thread.start()
        logger.info("Node %s started (region: %s)", self.node_id, self.region_name)

    def stop(self) -> None:
        self._running = False
        if self._gossip_thread:
            self._gossip_thread.join(timeout=2)

    # ------------------------------------------------------------------
    # Client API (AP: always accepts local writes)
    # ------------------------------------------------------------------

    def write(self, key: str, operation: str, **kwargs) -> dict:
        """Accept a write unconditionally — availability is preserved even during partition."""
        self._write_attempts += 1
        success = False

        if operation == "increment":
            success = self.store.increment(key, kwargs.get("amount", 1))
        elif operation == "decrement":
            success = self.store.decrement(key, kwargs.get("amount", 1))
        elif operation == "set":
            success = self.store.write(key, kwargs["value"], kwargs.get("timestamp"))
        elif operation == "add":
            success = self.store.add(key, kwargs["element"])
        elif operation == "remove":
            success = self.store.remove(key, kwargs["element"])
        elif operation == "create":
            self.store.create(key, kwargs["crdt_type"])
            success = True

        if success:
            self._write_successes += 1

        return {
            "node": self.node_id,
            "key": key,
            "success": success,
            "value": self.store.read(key),
            "partitioned": self._any_peer_partitioned(),
        }

    def read(self, key: str) -> dict:
        return {
            "node": self.node_id,
            "key": key,
            "value": self.store.read(key),
        }

    # ------------------------------------------------------------------
    # Gossip (anti-entropy)
    # ------------------------------------------------------------------

    def _gossip_loop(self) -> None:
        while self._running:
            time.sleep(self.gossip_interval_s)
            self._gossip_round()

    def _gossip_round(self) -> None:
        state = self.store.full_state()
        if not state:
            return

        # Fanout to all peers (or a random subset for large clusters)
        targets = self._peers

        for peer in targets:
            self._gossip_sent += 1
            payload = {"from": self.node_id, "state": state}

            self.network.send(
                src=self.node_id,
                dst=peer.node_id,
                payload=payload,
                callback=peer._receive_gossip,
            )

    def _receive_gossip(self, payload: dict) -> None:
        sender = payload["from"]
        remote_state = payload["state"]
        self._gossip_received += 1

        before = {k: self.store.read(k) for k in remote_state}
        for key, crdt_dict in remote_state.items():
            self.store.merge_remote(key, crdt_dict)
        after = {k: self.store.read(k) for k in remote_state}

        changed = {k for k in before if before[k] != after[k]}
        if changed:
            logger.debug(
                "Node %s merged from %s, keys changed: %s", self.node_id, sender, changed
            )
            for cb in self._convergence_callbacks:
                cb(self.node_id, changed)

    # ------------------------------------------------------------------
    # Convergence detection
    # ------------------------------------------------------------------

    def on_convergence(self, callback: Callable) -> None:
        self._convergence_callbacks.append(callback)

    def diverges_from(self, other: "RegionNode") -> Dict[str, tuple]:
        """Return keys where this node's value differs from another node."""
        my_keys = set(self.store.keys())
        their_keys = set(other.store.keys())
        all_keys = my_keys | their_keys
        diffs = {}
        for key in all_keys:
            my_val = self.store.read(key)
            their_val = other.store.read(key)
            if my_val != their_val:
                diffs[key] = (my_val, their_val)
        return diffs

    def is_converged_with(self, other: "RegionNode") -> bool:
        return len(self.diverges_from(other)) == 0

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def _any_peer_partitioned(self) -> bool:
        return any(self.network.is_partitioned(self.node_id, p.node_id) for p in self._peers)

    def metrics(self) -> dict:
        return {
            "node_id": self.node_id,
            "region": self.region_name,
            "write_attempts": self._write_attempts,
            "write_successes": self._write_successes,
            "write_availability": (
                self._write_successes / self._write_attempts
                if self._write_attempts > 0
                else 1.0
            ),
            "gossip_sent": self._gossip_sent,
            "gossip_received": self._gossip_received,
            "store_stats": self.store.stats(),
        }
