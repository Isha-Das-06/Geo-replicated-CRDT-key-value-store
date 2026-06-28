"""
Pre-defined network partition scenarios used by the benchmark suite.

Each scenario is a dict with:
  name          - human-readable label
  side_a        - nodes on one side of the partition
  side_b        - nodes on the other side
  partition_s   - how long the partition lasts (seconds)
  write_load    - writes per second per node during partition
  description   - narrative explanation
"""

SCENARIOS = [
    {
        "name": "single_region_isolated",
        "side_a": ["us-east"],
        "side_b": ["eu-west", "ap-south"],
        "partition_s": 5.0,
        "write_load": 20,
        "description": (
            "One region loses connectivity. The majority partition continues "
            "serving reads and writes. CAP: we choose A over C."
        ),
    },
    {
        "name": "transatlantic_cut",
        "side_a": ["us-east", "us-west"],
        "side_b": ["eu-west", "ap-south"],
        "partition_s": 5.0,
        "write_load": 20,
        "description": (
            "Simulates a transatlantic cable cut. Both partitions continue "
            "accepting writes independently and diverge temporarily."
        ),
    },
    {
        "name": "majority_minority_split",
        "side_a": ["us-east", "eu-west"],
        "side_b": ["ap-south"],
        "partition_s": 5.0,
        "write_load": 20,
        "description": (
            "Majority (2 nodes) vs minority (1 node) partition. "
            "All nodes stay available; minority diverges then converges on heal."
        ),
    },
    {
        "name": "flapping_partition",
        "side_a": ["us-east"],
        "side_b": ["eu-west", "ap-south"],
        "partition_s": 1.0,   # short partition repeated
        "write_load": 30,
        "description": (
            "Rapid connect/disconnect (flapping link). Tests robustness of "
            "gossip convergence under repeated partial failures."
        ),
    },
    {
        "name": "high_latency_no_partition",
        "side_a": [],
        "side_b": [],
        "partition_s": 0,
        "write_load": 20,
        "description": (
            "No partition — baseline measurement of convergence time under "
            "realistic cross-region latencies only."
        ),
    },
]
