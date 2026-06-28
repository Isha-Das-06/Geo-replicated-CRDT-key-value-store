"""
Main experiment runner.

Usage:
    python experiments/run_experiments.py [--quick] [--scenario NAME] [--crdt TYPE]

Flags:
    --quick       Run a fast subset (2 scenarios × 2 CRDTs)
    --scenario    Run only one named scenario
    --crdt        Run only one CRDT type
"""

import sys
import os
import argparse
import time
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from experiments.scenarios import SCENARIOS
from experiments.benchmark import run_all_benchmarks
from experiments.report import print_summary_table, generate_ascii_chart


def parse_args():
    p = argparse.ArgumentParser(description="CRDT geo-replication benchmark")
    p.add_argument("--quick", action="store_true", help="Run quick subset")
    p.add_argument("--scenario", type=str, help="Run one specific scenario by name")
    p.add_argument("--crdt", type=str, help="Run one specific CRDT type")
    return p.parse_args()


def main():
    args = parse_args()

    CRDT_TYPES = ["gcounter", "pncounter", "lwwregister", "orset"]

    scenarios = SCENARIOS
    crdt_types = CRDT_TYPES

    if args.scenario:
        scenarios = [s for s in SCENARIOS if s["name"] == args.scenario]
        if not scenarios:
            print(f"Unknown scenario: {args.scenario}")
            print(f"Available: {[s['name'] for s in SCENARIOS]}")
            sys.exit(1)

    if args.crdt:
        crdt_types = [args.crdt]

    if args.quick:
        scenarios = scenarios[:2]
        crdt_types = crdt_types[:2]

    print("=" * 70)
    print("  GEO-REPLICATED CRDT STORE -- PARTITION BENCHMARK")
    print("=" * 70)
    print(f"  Regions: US East | EU West | AP South")
    print(f"  Scenarios: {len(scenarios)}   CRDT types: {len(crdt_types)}")
    print(f"  Total runs: {len(scenarios) * len(crdt_types)}")
    print("=" * 70 + "\n")

    start = time.time()
    results = run_all_benchmarks(scenarios, crdt_types)
    elapsed = time.time() - start

    print(f"\n  Completed {len(results)} benchmarks in {elapsed:.1f}s\n")

    print_summary_table(results)
    generate_ascii_chart(results)

    return results


if __name__ == "__main__":
    main()
