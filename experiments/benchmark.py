"""
Core benchmark harness.

For each scenario and CRDT type, measures:
  - Write availability during partition (fraction of writes that succeed)
  - Convergence time after partition heals
  - Divergence depth (max value difference across nodes at peak divergence)

Results are written to results/benchmark_results.json.
"""

import sys
import os
import json
import time
import threading
import logging
import statistics
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.cluster.coordinator import ClusterCoordinator
from src.network.simulator import NetworkCondition

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

REGIONS = [
    ("us-east", "US East (N. Virginia)"),
    ("eu-west", "EU West (Ireland)"),
    ("ap-south", "AP South (Mumbai)"),
]


@dataclass
class BenchmarkResult:
    scenario: str
    crdt_type: str
    partition_duration_s: float
    write_availability: float           # fraction of writes that succeeded
    convergence_time_s: Optional[float] # None if did not converge in timeout
    peak_divergence: float              # max value gap across nodes at partition peak
    writes_during_partition: int
    writes_after_heal: int
    network_stats: Dict = field(default_factory=dict)
    node_metrics: Dict = field(default_factory=dict)
    description: str = ""


def _run_writes(
    node,
    key: str,
    crdt_type: str,
    rate_per_s: int,
    duration_s: float,
    stop_event: threading.Event,
    results: list,
) -> None:
    interval = 1.0 / rate_per_s
    count = 0
    while not stop_event.is_set():
        if crdt_type == "gcounter":
            node.write(key, "increment", amount=1)
        elif crdt_type == "pncounter":
            if count % 3 == 0:
                node.write(key, "decrement", amount=1)
            else:
                node.write(key, "increment", amount=1)
        elif crdt_type == "lwwregister":
            node.write(key, "set", value=f"val-{node.node_id}-{count}", timestamp=time.time())
        elif crdt_type == "orset":
            node.write(key, "add", element=f"item-{node.node_id}-{count}")
        count += 1
        results.append(1)
        time.sleep(interval)


def run_single_benchmark(scenario: dict, crdt_type: str) -> BenchmarkResult:
    cluster = ClusterCoordinator(gossip_interval_s=0.2)

    for node_id, region_name in REGIONS:
        cluster.add_node(node_id, region_name)

    cluster.wire_realistic_latencies(jitter_pct=0.1)
    cluster.connect_all_peers()
    cluster.start_all()

    # Initialise the CRDT key on all nodes
    key = f"bench-{crdt_type}"
    for node in cluster.nodes.values():
        node.write(key, "create", crdt_type=crdt_type)

    time.sleep(0.3)  # let gossip distribute the key

    writes_during: list = []
    writes_after: list = []
    stop_event = threading.Event()

    # ------------------------------------------------------------------
    # Phase 1: inject partition and run writes
    # ------------------------------------------------------------------
    if scenario["side_a"] and scenario["side_b"]:
        cluster.partition(scenario["side_a"], scenario["side_b"])

    write_threads = []
    for node in cluster.nodes.values():
        t = threading.Thread(
            target=_run_writes,
            args=(node, key, crdt_type, scenario["write_load"],
                  scenario["partition_s"], stop_event, writes_during),
            daemon=True,
        )
        t.start()
        write_threads.append(t)

    partition_start = time.time()
    time.sleep(scenario["partition_s"] if scenario["partition_s"] > 0 else 2.0)
    stop_event.set()
    for t in write_threads:
        t.join()

    # Snapshot divergence at partition peak
    node_values = list(cluster.cluster_values(key).values())
    try:
        numeric_values = [v for v in node_values if isinstance(v, (int, float))]
        peak_divergence = (max(numeric_values) - min(numeric_values)) if len(numeric_values) > 1 else 0
    except (TypeError, ValueError):
        peak_divergence = float(len(set(str(v) for v in node_values)) - 1)

    # Write availability during partition
    total_write_attempts = sum(n.metrics()["write_attempts"] for n in cluster.nodes.values())
    total_write_successes = sum(n.metrics()["write_successes"] for n in cluster.nodes.values())
    write_availability = total_write_successes / total_write_attempts if total_write_attempts else 1.0

    # ------------------------------------------------------------------
    # Phase 2: heal and measure convergence
    # ------------------------------------------------------------------
    cluster.heal()
    heal_start = time.time()

    # Collect post-heal writes
    stop_event2 = threading.Event()
    post_threads = []
    for node in cluster.nodes.values():
        t = threading.Thread(
            target=_run_writes,
            args=(node, key, crdt_type, scenario["write_load"],
                  2.0, stop_event2, writes_after),
            daemon=True,
        )
        t.start()
        post_threads.append(t)

    convergence_time = cluster.wait_for_convergence(key, timeout_s=15.0)
    stop_event2.set()
    for t in post_threads:
        t.join()

    cluster.stop_all()

    return BenchmarkResult(
        scenario=scenario["name"],
        crdt_type=crdt_type,
        partition_duration_s=scenario["partition_s"],
        write_availability=write_availability,
        convergence_time_s=convergence_time,
        peak_divergence=peak_divergence,
        writes_during_partition=len(writes_during),
        writes_after_heal=len(writes_after),
        network_stats=cluster.network_stats(),
        node_metrics=cluster.cluster_metrics(),
        description=scenario["description"],
    )


def run_all_benchmarks(scenarios, crdt_types, output_path: str = "results/benchmark_results.json") -> List[BenchmarkResult]:
    results = []
    total = len(scenarios) * len(crdt_types)
    idx = 0

    for scenario in scenarios:
        for crdt_type in crdt_types:
            idx += 1
            print(f"  [{idx:2d}/{total}] scenario={scenario['name']:30s}  crdt={crdt_type}")
            try:
                result = run_single_benchmark(scenario, crdt_type)
                results.append(result)
                conv = f"{result.convergence_time_s:.3f}s" if result.convergence_time_s else "TIMEOUT"
                print(f"          avail={result.write_availability:.1%}  "
                      f"convergence={conv}  "
                      f"peak_divergence={result.peak_divergence}")
            except Exception as exc:
                print(f"          ERROR: {exc}")
                logging.exception("Benchmark failed")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        json.dump([asdict(r) for r in results], f, indent=2, default=str)

    print(f"\n  Results written to {output_path}")
    return results
