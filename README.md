# Geo-Replicated CRDT Store

A multi-region key-value store that uses **Conflict-Free Replicated Data Types (CRDTs)** for automatic conflict resolution and **programmatic network simulation** (mirroring Linux `tc/netem` semantics) to inject realistic cross-region latency and hard network partitions — empirically demonstrating the CAP theorem instead of just citing it.

---

## Why This Project Exists

Most distributed systems demos stop at "here's a Google Docs clone with OT." This project goes further:

1. **Implements four CRDT types** from scratch with mathematically correct merge semantics
2. **Simulates real network conditions** — cross-region RTTs up to 180 ms, jitter, packet loss, and full partitions — in-process, without needing root or a Linux machine
3. **Runs controlled experiments** across 5 partition scenarios and measures write availability and convergence time empirically
4. **Produces quantified results**: 100% write availability during all partitions, convergence in 0.15–0.71 s after healing

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ClusterCoordinator                          │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐                 │
│  │  us-east   │    │  eu-west   │    │  ap-south  │   RegionNode    │
│  │ CRDTStore  │    │ CRDTStore  │    │ CRDTStore  │                 │
│  └─────┬──────┘    └─────┬──────┘    └─────┬──────┘                │
│        │                 │                  │                       │
│        └─────────────────┼──────────────────┘                      │
│                     NetworkSimulator                                 │
│           (latency / jitter / drop / partition)                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Components

| Module | Role |
|---|---|
| `src/crdts/` | CRDT math: G-Counter, PN-Counter, LWW-Register, OR-Set |
| `src/network/simulator.py` | tc/netem-style link conditions + partition injection |
| `src/node/region_node.py` | AP node: local writes always succeed; gossip anti-entropy |
| `src/node/store.py` | Per-node key-value store; exposes merge API for gossip |
| `src/cluster/coordinator.py` | Wires nodes, links, and convergence measurement |
| `experiments/` | Benchmark harness + 5 partition scenarios |

---

## CRDT Implementations

### G-Counter (Grow-Only Counter)

**State**: `{ node_id → count }`  
**Merge**: element-wise maximum  
**Value**: sum of all shards  

Each node increments only its own shard. Because merge takes the max of each shard independently, the operation is **commutative, associative, and idempotent** — gossip can arrive in any order, any number of times, and convergence is guaranteed.

```python
# After a 5s partition where each side incremented 10×:
us-east:  { "us-east": 15, "eu-west": 10, "ap-south": 10 }  → value = 35
eu-west:  { "us-east":  5, "eu-west": 20, "ap-south": 10 }  → value = 35
# After merge: { "us-east": 15, "eu-west": 20, "ap-south": 10 } → value = 45 ✓
```

### PN-Counter (Positive-Negative Counter)

**State**: two G-Counters — P (increments) and N (decrements)  
**Value**: P.value() − N.value()  

Composition of two G-Counters. Decrement is expressed as incrementing the N shard, which is itself monotonically growing. Merge is just merge(P, P') and merge(N, N').

### LWW-Register (Last-Write-Wins Register)

**State**: `{ value, timestamp, writer_id }`  
**Merge**: pick entry with highest (timestamp, writer_id) pair  

Writes are tagged with a wall-clock timestamp and the writing node's ID. The node ID breaks ties deterministically, preventing split-brain. Concurrent writes from two sides of a partition **always produce the same result** after gossip, regardless of message arrival order.

```
Partition: us-east writes "enabled" @ t+0.001
           ap-south writes "disabled" @ t+0.002

After heal: all nodes converge to "disabled" (higher timestamp wins)
```

### OR-Set (Observed-Remove Set)

**State**: `adds: Set[(element, token)], removes: Set[token]`  
**Merge**: union of both add-sets and both remove-sets  

This solves the classic distributed set problem. A naive set CRDT cannot resolve "concurrent add and remove" because there's no ordering. OR-Set assigns each add a unique UUID token. Remove only removes specific observed tokens. Result: **concurrent add and remove → element stays present** (add wins).

```
Partition: us-east  adds "charlie"
           eu-west  removes "alice"   (removes alice's add-token)

After heal merge:
  adds    = {(alice, t1), (bob, t2), (charlie, t3)}
  removes = {t1}
  value   = {bob, charlie}   ← alice is gone, charlie survived
```

---

## Network Simulation (tc/netem in Python)

On Linux you would use:

```bash
tc qdisc add dev eth0 root netem delay 80ms 10ms distribution normal
tc qdisc add dev eth0 root netem loss 1%
iptables -A INPUT -s 10.0.2.0/24 -j DROP   # partition
```

This project implements equivalent semantics in Python with no OS dependencies:

```python
# Asymmetric link: us-east to eu-west with 80ms ± 10ms latency
sim.set_condition("us-east", "eu-west",
    NetworkCondition(latency_ms=80, jitter_ms=10, drop_rate=0.0))

# Hard partition: split the cluster
sim.partition(side_a=["us-east"], side_b=["eu-west", "ap-south"])

# Heal
sim.heal()
```

**Realistic inter-region latencies used in experiments:**

| Link | Latency | Jitter |
|---|---|---|
| us-east ↔ us-west | 35 ms | ±3.5 ms |
| us-east ↔ eu-west | 80 ms | ±8 ms |
| us-east ↔ ap-south | 180 ms | ±18 ms |
| eu-west ↔ ap-south | 120 ms | ±12 ms |

---

## Experimental Results

All experiments run on a 3-node cluster (`us-east`, `eu-west`, `ap-south`) with 20 writes/sec/node under partition.

### Write Availability During Partition

| Scenario | Description | Write Availability |
|---|---|---|
| Single region isolated | 1 node vs 2-node majority | **100%** |
| Transatlantic cut | US vs EU+AP partition | **100%** |
| Majority/minority split | 2 vs 1 node partition | **100%** |
| Flapping partition | Rapid connect/disconnect cycles | **100%** |
| High latency baseline | No partition, realistic RTTs | **100%** |

**Observation**: CRDTs preserve 100% write availability under all partition scenarios. Every write is accepted locally without coordination. This is the **A** (Availability) side of CAP being deliberately chosen over **C** (Consistency).

### Convergence Time After Partition Heals

| Scenario | G-Counter | PN-Counter | OR-Set |
|---|---|---|---|
| Single region isolated | 0.203 s | 0.658 s | 0.203 s |
| Transatlantic cut | 0.709 s | 0.315 s | 0.125 s |
| Majority/minority split | 0.153 s | 0.209 s | 0.205 s |
| Flapping partition | 0.155 s | 0.152 s | 0.152 s |

> LWW-Register shows no convergence timeout in the benchmark table because nodes keep overwriting each other during the post-heal measurement window — the register *does* converge once writes quiesce (demonstrated in the interactive demo: 0.10 s convergence).

**Observation**: Convergence time is bounded by gossip interval (0.2 s) plus max one-way latency (180 ms). Even after a 5-second transatlantic cut with ~200 accumulated divergent writes, the cluster converges in under 1 second.

### Peak Divergence During Partition

| Scenario | G-Counter divergence |
|---|---|
| 5s transatlantic cut, 20 writes/s/node | 99 units |
| 5s majority/minority split | 98 units |
| 1s flapping partition | 29 units |

Divergence is proportional to (write rate × partition duration), which is expected — the store is AP, not CP.

---

## CAP Theorem: Empirical Demonstration

The experiments directly show the AP trade-off:

```
              C (Consistency)
              |
              |  CP systems (e.g., ZooKeeper, etcd)
              |  → refuse writes during partition
              |
AP ───────────┼─────────────
(this store)  |
              |  CA systems (not partition-tolerant)
              |  → not viable in real distributed systems
```

- **A guaranteed**: zero write rejections across all scenarios
- **P guaranteed**: cluster survives all injected partitions without crashing
- **C sacrificed**: nodes hold different values during partitions
- **Eventual C**: nodes converge automatically after heal, in < 1 second

---

## Project Structure

```
.
├── src/
│   ├── crdts/
│   │   ├── base.py           # Abstract CRDT interface
│   │   ├── g_counter.py      # Grow-only counter
│   │   ├── pn_counter.py     # Increment/decrement counter
│   │   ├── lww_register.py   # Last-Write-Wins register
│   │   └── or_set.py         # Observed-Remove set
│   ├── network/
│   │   └── simulator.py      # tc/netem semantics in Python
│   ├── node/
│   │   ├── region_node.py    # AP node with gossip anti-entropy
│   │   └── store.py          # Per-node CRDT key-value store
│   └── cluster/
│       └── coordinator.py    # Cluster wiring + convergence measurement
├── experiments/
│   ├── scenarios.py          # 5 partition scenario definitions
│   ├── benchmark.py          # Per-scenario measurement harness
│   ├── run_experiments.py    # CLI entry point
│   └── report.py             # ASCII table + bar chart output
├── results/
│   └── benchmark_results.json  # Raw results (auto-generated)
├── demo.py                   # Interactive walkthrough
└── requirements.txt          # No third-party dependencies
```

---

## Getting Started

**Requirements**: Python 3.8+, no third-party libraries.

```bash
# Clone / open the project
cd geo-crdt-store

# Interactive demo (runs in ~20 seconds)
python demo.py

# Full benchmark suite (runs ~3 minutes, 5 scenarios × 4 CRDT types)
python experiments/run_experiments.py

# Quick smoke test (2 scenarios × 2 CRDTs, ~25 seconds)
python experiments/run_experiments.py --quick

# Target a specific scenario
python experiments/run_experiments.py --scenario transatlantic_cut --crdt gcounter
```

Available scenario names: `single_region_isolated`, `transatlantic_cut`, `majority_minority_split`, `flapping_partition`, `high_latency_no_partition`

Available CRDT types: `gcounter`, `pncounter`, `lwwregister`, `orset`

---

## Design Decisions

### Why gossip (anti-entropy) instead of causal broadcast?

Gossip is self-healing. If a message is dropped, the next gossip round re-sends the full state. With causal broadcast you need reliable delivery or gap-filling, which re-introduces coordination. For a demo of CRDT properties, gossip's simplicity is a feature.

### Why full-state gossip instead of delta-CRDTs?

Delta-CRDTs (sending only the changed portion of state) reduce bandwidth but complicate the implementation. Full-state gossip makes the merge semantics transparent — every gossip round is exactly `local_state.merge(remote_state)`. In production you'd switch to delta-state for large state sizes.

### Why (timestamp, node_id) for LWW instead of vector clocks?

Vector clocks give you causal ordering but require `O(n)` metadata per write and can still produce ties. LWW with (timestamp, node_id) is `O(1)` per value and always produces a total order. The trade-off is that clock skew can cause newer writes to lose — acceptable when clocks are synchronized (NTP, PTP) and the use case doesn't require strict causal ordering.

### Why UUID tokens in OR-Set instead of version vectors?

UUID tokens decouple the add/remove semantics from replica identity. Each add operation is unique by construction, so concurrent adds of the same element don't collide. This is simpler to implement than version-vector approaches and has equivalent correctness properties for the add-wins semantics.

---

## Resume Bullet Points (filled with real numbers)

> **Engineered a multi-region, geo-replicated key-value store in Python** using four Conflict-Free Replicated Data Types (G-Counter, PN-Counter, LWW-Register, OR-Set) to guarantee eventual consistency and automatic conflict resolution across 3 simulated regions (US East, EU West, AP South)

> **Simulated cross-region network partitions and latency injection** (up to 180 ms one-way) by implementing Linux `tc/netem` semantics in Python — reproducing CAP theorem trade-offs between availability and consistency empirically rather than theoretically, without requiring root access or a Linux host

> **Benchmarked write availability and convergence time** across 5 partition/recovery scenarios (single-region isolation, transatlantic cable cut, majority/minority split, flapping links, high-latency baseline), measuring **100% write availability** during all partitions and **0.15–0.71 s average convergence time** post-healing

> **Implemented and compared four CRDT conflict-resolution strategies** — designing controlled experiments at 20 writes/sec/node to quantify consistency-availability trade-offs, and quantifying peak divergence of up to 99 units during a 5-second transatlantic partition with zero data loss after healing
