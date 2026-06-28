"""
Network condition simulator that mimics Linux tc/netem behavior in Python.

tc/netem (traffic control / network emulator) is the Linux kernel facility
that lets you inject latency, jitter, packet loss, and partitions on a
per-interface basis. This module replicates those semantics in-process so
the same experiments can run on any OS.

Key concepts mirrored from tc/netem:
  - netem delay <time> <jitter>   -> latency_ms ± jitter_ms (normal distribution)
  - netem loss <pct>%             -> drop_rate (0.0 – 1.0)
  - iptables DROP / tc filter     -> partition (complete connectivity loss)
"""

import random
import threading
import time
import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class NetworkCondition:
    """Per-link network condition, analogous to a tc/netem qdisc rule."""

    latency_ms: float = 0.0       # base one-way latency
    jitter_ms: float = 0.0        # ± jitter drawn from normal distribution
    drop_rate: float = 0.0        # packet-loss probability [0, 1]
    bandwidth_kbps: float = 0.0   # 0 means unlimited

    def sample_delay(self) -> float:
        """Return a sampled delay in seconds, always >= 0."""
        if self.jitter_ms > 0:
            delay = random.gauss(self.latency_ms, self.jitter_ms)
        else:
            delay = self.latency_ms
        return max(0.0, delay) / 1000.0

    def should_drop(self) -> bool:
        return self.drop_rate > 0 and random.random() < self.drop_rate


@dataclass
class PartitionConfig:
    """Describes a network partition between two sets of nodes."""

    side_a: list = field(default_factory=list)
    side_b: list = field(default_factory=list)
    active: bool = False

    def is_partitioned(self, src: str, dst: str) -> bool:
        if not self.active:
            return False
        return (src in self.side_a and dst in self.side_b) or (
            src in self.side_b and dst in self.side_a
        )


class NetworkSimulator:
    """
    Intercepts inter-node message delivery and applies tc/netem-style conditions.

    Usage:
        sim = NetworkSimulator()
        sim.set_condition("us-east", "eu-west", NetworkCondition(latency_ms=80, jitter_ms=10))
        sim.partition(["us-east"], ["eu-west", "ap-south"])

        # To send a message from node A to node B:
        sim.send("us-east", "eu-west", payload, callback)

        # Heal the partition:
        sim.heal()
    """

    def __init__(self):
        self._conditions: Dict[Tuple[str, str], NetworkCondition] = {}
        self._partition = PartitionConfig()
        self._lock = threading.RLock()
        self._stats: Dict[str, int] = {
            "sent": 0,
            "dropped": 0,
            "partitioned": 0,
            "delivered": 0,
        }

    # ------------------------------------------------------------------
    # Configuration API
    # ------------------------------------------------------------------

    def set_condition(self, src: str, dst: str, condition: NetworkCondition) -> None:
        """Set asymmetric link condition from src to dst."""
        with self._lock:
            self._conditions[(src, dst)] = condition
            logger.debug("Link %s->%s: %s", src, dst, condition)

    def set_symmetric_condition(
        self, node_a: str, node_b: str, condition: NetworkCondition
    ) -> None:
        self.set_condition(node_a, node_b, condition)
        self.set_condition(node_b, node_a, condition)

    def partition(self, side_a: list, side_b: list) -> None:
        """Split the network. Traffic between side_a and side_b is dropped."""
        with self._lock:
            self._partition = PartitionConfig(side_a=list(side_a), side_b=list(side_b), active=True)
        logger.info("PARTITION injected: %s | %s", side_a, side_b)

    def heal(self) -> None:
        """Remove the partition — all nodes can communicate again."""
        with self._lock:
            self._partition.active = False
        logger.info("PARTITION healed — network restored")

    def get_condition(self, src: str, dst: str) -> NetworkCondition:
        with self._lock:
            return self._conditions.get((src, dst), NetworkCondition())

    def is_partitioned(self, src: str, dst: str) -> bool:
        with self._lock:
            return self._partition.is_partitioned(src, dst)

    # ------------------------------------------------------------------
    # Message delivery
    # ------------------------------------------------------------------

    def send(
        self,
        src: str,
        dst: str,
        payload: dict,
        callback: Callable[[dict], None],
        error_callback: Optional[Callable[[str], None]] = None,
    ) -> None:
        """
        Deliver payload from src to dst asynchronously, applying link conditions.
        callback is called on the delivery thread after the simulated delay.
        """
        with self._lock:
            self._stats["sent"] += 1
            partitioned = self._partition.is_partitioned(src, dst)
            condition = self._conditions.get((src, dst), NetworkCondition())

        if partitioned:
            with self._lock:
                self._stats["partitioned"] += 1
            logger.debug("DROP (partition) %s->%s", src, dst)
            if error_callback:
                error_callback("partitioned")
            return

        if condition.should_drop():
            with self._lock:
                self._stats["dropped"] += 1
            logger.debug("DROP (loss) %s->%s", src, dst)
            if error_callback:
                error_callback("dropped")
            return

        delay = condition.sample_delay()

        def _deliver():
            if delay > 0:
                time.sleep(delay)
            with self._lock:
                self._stats["delivered"] += 1
            try:
                callback(payload)
            except Exception as exc:
                logger.warning("Delivery callback error %s->%s: %s", src, dst, exc)

        t = threading.Thread(target=_deliver, daemon=True)
        t.start()

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        with self._lock:
            return dict(self._stats)

    def reset_stats(self) -> None:
        with self._lock:
            self._stats = {k: 0 for k in self._stats}

    def partition_active(self) -> bool:
        with self._lock:
            return self._partition.active

    def partition_sides(self) -> Tuple[list, list]:
        with self._lock:
            return self._partition.side_a[:], self._partition.side_b[:]
