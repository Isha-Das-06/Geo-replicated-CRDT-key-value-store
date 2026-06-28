"""
Terminal report formatter for benchmark results.
Generates ASCII tables and bar charts — no matplotlib dependency.
"""

from typing import List
from experiments.benchmark import BenchmarkResult


def print_summary_table(results: List[BenchmarkResult]) -> None:
    print("=" * 90)
    print(f"  {'SCENARIO':<32} {'CRDT':<14} {'AVAIL':>7} {'CONV(s)':>9} {'PEAK DIV':>10}")
    print("-" * 90)

    for r in results:
        conv = f"{r.convergence_time_s:.3f}" if r.convergence_time_s is not None else "TIMEOUT"
        avail = f"{r.write_availability:.1%}"
        print(
            f"  {r.scenario:<32} {r.crdt_type:<14} {avail:>7} {conv:>9} {r.peak_divergence:>10.1f}"
        )
    print("=" * 90)


def generate_ascii_chart(results: List[BenchmarkResult]) -> None:
    """Print ASCII bar chart of convergence times by scenario."""
    by_scenario = {}
    for r in results:
        if r.convergence_time_s is not None:
            by_scenario.setdefault(r.scenario, []).append(r.convergence_time_s)

    if not by_scenario:
        return

    max_val = max(max(v) for v in by_scenario.values())
    bar_width = 40

    print("\n  CONVERGENCE TIME BY SCENARIO (avg across CRDT types)")
    print("  " + "-" * 70)

    for scenario, times in by_scenario.items():
        avg = sum(times) / len(times)
        filled = int((avg / max_val) * bar_width) if max_val > 0 else 0
        bar = "#" * filled + "." * (bar_width - filled)
        print(f"  {scenario:<32} |{bar}| {avg:.3f}s")

    print()

    # Write availability chart
    avail_by_scenario = {}
    for r in results:
        avail_by_scenario.setdefault(r.scenario, []).append(r.write_availability)

    print("  WRITE AVAILABILITY BY SCENARIO (avg across CRDT types)")
    print("  " + "-" * 70)

    for scenario, avails in avail_by_scenario.items():
        avg = sum(avails) / len(avails)
        filled = int(avg * bar_width)
        bar = "#" * filled + "." * (bar_width - filled)
        print(f"  {scenario:<32} |{bar}| {avg:.1%}")

    print()
