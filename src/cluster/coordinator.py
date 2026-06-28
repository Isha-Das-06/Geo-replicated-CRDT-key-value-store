"""
Cluster coordinator — wires together nodes and the network simulator.

Provides high-level operations like spinning up a 3-region cluster,
injecting partitions, and waiting for convergence.
"""

import time
import logging
from typing import Dict, List, Optional

from src.node.region_node import RegionNode
from src.network.simulator import NetworkSimulator, NetworkCondition

logger = logging.getLogger(__name__)


# Real-world cross-region RTTs (one-way latency in ms)
REGION_LATENCIES = {
    ("us-east", "us-west"):   35,
    ("us-east", "eu-west"):   80,
    ("us-east", "ap-south"):  180,
    ("us-west", "eu-west"):   130,
    ("us-west", "ap-south"):  150,
    ("eu-west", "ap-south"):  120,
}


class ClusterCoordinator:
    def __init__(self, gossip_interval_s: float = 0.5):
        self.network = NetworkSimulator()
        self.nodes: Dict[str, RegionNode] = {}
        self.gossip_interval_s = gossip_interval_s

    # ------------------------------------------------------------------
    # Cluster setup
    # ------------------------------------------------------------------

    def add_node(self, node_id: str, region_name: str) -> RegionNode:
        node = RegionNode(node_id, region_name, self.network, self.gossip_interval_s)
        self.nodes[node_id] = node
        return node

    def wire_realistic_latencies(self, jitter_pct: float = 0.1) -> None:
        """Apply real-world inter-region latencies to all links."""
        node_list = list(self.nodes.values())
        for i, a in enumerate(node_list):
            for b in node_list[i + 1 :]:
                key = tuple(sorted([a.region_name, b.region_name]))
                latency = REGION_LATENCIES.get(key, 50)
                jitter = latency * jitter_pct
                cond = NetworkCondition(latency_ms=latency, jitter_ms=jitter)
                self.network.set_symmetric_condition(a.node_id, b.node_id, cond)
                logger.debug(
                    "Link %s<->%s: %dms ±%.0fms", a.node_id, b.node_id, latency, jitter
                )

    def connect_all_peers(self) -> None:
        node_list = list(self.nodes.values())
        for node in node_list:
            for peer in node_list:
                if peer is not node:
                    node.add_peer(peer)

    def start_all(self) -> None:
        for node in self.nodes.values():
            node.start()

    def stop_all(self) -> None:
        for node in self.nodes.values():
            node.stop()

    # ------------------------------------------------------------------
    # Partition control
    # ------------------------------------------------------------------

    def partition(self, side_a: List[str], side_b: List[str]) -> None:
        self.network.partition(side_a, side_b)

    def heal(self) -> None:
        self.network.heal()

    # ------------------------------------------------------------------
    # Convergence utilities
    # ------------------------------------------------------------------

    def wait_for_convergence(
        self,
        key: str,
        timeout_s: float = 30.0,
        poll_interval_s: float = 0.05,
    ) -> Optional[float]:
        """
        Block until all nodes agree on the value for key, or timeout.
        Returns the number of seconds it took, or None on timeout.
        """
        start = time.time()
        deadline = start + timeout_s

        while time.time() < deadline:
            values = {node_id: node.store.read(key) for node_id, node in self.nodes.items()}
            unique_values = set(
                v if not isinstance(v, (set, list)) else frozenset(v)
                for v in values.values()
            )
            if len(unique_values) == 1:
                return time.time() - start
            time.sleep(poll_interval_s)

        return None

    def wait_for_full_convergence(
        self, timeout_s: float = 30.0, poll_interval_s: float = 0.05
    ) -> Optional[float]:
        """Wait until all keys on all nodes converge."""
        start = time.time()
        deadline = start + timeout_s
        node_list = list(self.nodes.values())

        while time.time() < deadline:
            converged = all(
                node_list[0].is_converged_with(other) for other in node_list[1:]
            )
            if converged:
                return time.time() - start
            time.sleep(poll_interval_s)

        return None

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def cluster_values(self, key: str) -> Dict[str, any]:
        return {node_id: node.store.read(key) for node_id, node in self.nodes.items()}

    def cluster_metrics(self) -> Dict[str, dict]:
        return {node_id: node.metrics() for node_id, node in self.nodes.items()}

    def network_stats(self) -> dict:
        return self.network.stats()

    def print_cluster_state(self, key: str = None) -> None:
        print("\n" + "=" * 60)
        print(f"  CLUSTER STATE")
        print("=" * 60)
        for node_id, node in self.nodes.items():
            m = node.metrics()
            print(
                f"  [{node_id:12s}] region={node.region_name:10s} "
                f"writes={m['write_successes']}/{m['write_attempts']} "
                f"avail={m['write_availability']:.1%}"
            )
            if key:
                print(f"               {key} = {node.store.read(key)}")
        net = self.network.stats()
        print(f"\n  Network: sent={net['sent']} delivered={net['delivered']} "
              f"partitioned={net['partitioned']} dropped={net['dropped']}")
        print("=" * 60 + "\n")
