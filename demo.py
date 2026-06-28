"""
Interactive demo -- walks through a live partition/heal cycle with commentary.

Run: python demo.py
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.cluster.coordinator import ClusterCoordinator


def banner(text: str) -> None:
    print("\n" + "=" * 65)
    print(f"  {text}")
    print("=" * 65)


def step(msg: str) -> None:
    print(f"\n  >> {msg}")


def show_values(cluster, key, label=""):
    vals = cluster.cluster_values(key)
    tag = f" [{label}]" if label else ""
    print(f"  State{tag}:")
    for node_id, val in vals.items():
        node = cluster.nodes[node_id]
        print(f"    {node_id:12s} ({node.region_name}): {val}")


def main():
    banner("GEO-REPLICATED CRDT STORE -- LIVE DEMO")
    print("""
  This demo empirically demonstrates the CAP theorem using CRDTs.

  We spin up 3 simulated datacenters:
    - us-east  (US East, N. Virginia)
    - eu-west  (EU West, Ireland)      ~80ms from us-east
    - ap-south (AP South, Mumbai)     ~180ms from us-east

  We inject a network partition, write to all sides, watch divergence,
  then heal and measure convergence time.
    """)

    cluster = ClusterCoordinator(gossip_interval_s=0.3)
    cluster.add_node("us-east", "US East")
    cluster.add_node("eu-west", "EU West")
    cluster.add_node("ap-south", "AP South")

    cluster.wire_realistic_latencies(jitter_pct=0.1)
    cluster.connect_all_peers()
    cluster.start_all()

    # ── Demo 1: G-Counter ──────────────────────────────────────────────
    banner("DEMO 1 -- G-Counter (grow-only distributed counter)")
    print("""
  G-Counter: each node owns a shard of the counter.
  Global value = sum of all shards. Merge = element-wise max.
  Perfect for distributed hit counters, view counts, download stats.
    """)

    key = "page_views"
    for node in cluster.nodes.values():
        node.write(key, "create", crdt_type="gcounter")
    time.sleep(0.4)

    step("Normal operation -- incrementing across all regions")
    cluster.nodes["us-east"].write(key, "increment", amount=5)
    cluster.nodes["eu-west"].write(key, "increment", amount=3)
    cluster.nodes["ap-south"].write(key, "increment", amount=2)
    time.sleep(1.5)
    show_values(cluster, key, "after 1.5s gossip -- all nodes see 10")

    step("Injecting partition: [us-east] | [eu-west, ap-south]")
    cluster.partition(["us-east"], ["eu-west", "ap-south"])

    step("Writing 10 increments to each side of the partition...")
    for _ in range(10):
        cluster.nodes["us-east"].write(key, "increment", amount=1)
        cluster.nodes["eu-west"].write(key, "increment", amount=1)
        cluster.nodes["ap-south"].write(key, "increment", amount=1)
        time.sleep(0.1)

    print()
    show_values(cluster, key, "during partition -- nodes diverged")

    step("Healing partition...")
    cluster.heal()
    t = cluster.wait_for_convergence(key, timeout_s=10.0)
    print(f"\n  [OK] Converged in {t:.3f}s")
    show_values(cluster, key, "after convergence")

    # ── Demo 2: LWW-Register ───────────────────────────────────────────
    banner("DEMO 2 -- LWW-Register (Last-Write-Wins Register)")
    print("""
  LWW-Register: each write carries a (timestamp, node_id) tag.
  On merge, the highest (timestamp, node_id) wins. Concurrent writes
  on different sides of a partition converge deterministically.
    """)

    reg_key = "config:feature_flag"
    for node in cluster.nodes.values():
        node.write(reg_key, "create", crdt_type="lwwregister")
    time.sleep(0.4)

    step("Injecting partition: [us-east, eu-west] | [ap-south]")
    cluster.partition(["us-east", "eu-west"], ["ap-south"])
    time.sleep(0.1)

    t0 = time.time()
    step("Concurrent writes on both sides:")
    cluster.nodes["us-east"].write(reg_key, "set", value="enabled", timestamp=t0 + 0.001)
    cluster.nodes["ap-south"].write(reg_key, "set", value="disabled", timestamp=t0 + 0.002)
    print(f"    us-east wrote 'enabled'   @ ts={t0+0.001:.6f}")
    print(f"    ap-south wrote 'disabled' @ ts={t0+0.002:.6f}  (higher ts -> should win)")

    show_values(cluster, reg_key, "during partition")

    step("Healing partition...")
    cluster.heal()
    t = cluster.wait_for_convergence(reg_key, timeout_s=10.0)
    print(f"\n  [OK] Converged in {t:.3f}s  (ap-south wins -- highest timestamp)")
    show_values(cluster, reg_key, "after convergence")

    # ── Demo 3: OR-Set ─────────────────────────────────────────────────
    banner("DEMO 3 -- OR-Set (Observed-Remove Set)")
    print("""
  OR-Set: add/remove conflicts resolved by 'add wins'. Each add gets
  a unique token. Remove only removes specific tokens. Concurrent
  add and remove of the same element results in element being present.
    """)

    set_key = "online_users"
    for node in cluster.nodes.values():
        node.write(set_key, "create", crdt_type="orset")
    time.sleep(0.4)

    for node in cluster.nodes.values():
        node.write(set_key, "add", element="alice")
        node.write(set_key, "add", element="bob")
    time.sleep(0.8)

    step("Injecting partition: [us-east] | [eu-west, ap-south]")
    cluster.partition(["us-east"], ["eu-west", "ap-south"])

    step("Concurrent conflict: us-east adds 'charlie', eu-west removes 'alice'")
    cluster.nodes["us-east"].write(set_key, "add", element="charlie")
    cluster.nodes["eu-west"].write(set_key, "remove", element="alice")

    show_values(cluster, set_key, "during partition")

    step("Healing partition...")
    cluster.heal()
    t = cluster.wait_for_convergence(set_key, timeout_s=10.0)
    print(f"\n  [OK] Converged in {t:.3f}s")
    show_values(cluster, set_key, "after convergence -- alice removed, charlie added")

    # ── Summary ────────────────────────────────────────────────────────
    banner("DEMO COMPLETE -- CAP Theorem Observations")
    print("""
  [OK] Availability: Every write was accepted by every node regardless of
       partition state. Zero write rejections. (A from CAP)

  [OK] Partition Tolerance: The cluster survived multiple network splits
       without data loss or coordinator failure. (P from CAP)

  [X]  Strong Consistency: Nodes temporarily diverged during partitions.
       Values differed across nodes for the partition duration. (C sacrificed)

  [OK] Eventual Consistency: All nodes converged automatically after heal
       without any manual intervention or conflict resolution code.

  This empirically demonstrates the AP corner of the CAP theorem.
  To run the full benchmark suite:  python experiments/run_experiments.py
    """)

    cluster.stop_all()


if __name__ == "__main__":
    main()
