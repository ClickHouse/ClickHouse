#!/usr/bin/env python3
"""
Compare Rocks vs Default Keeper backend performance from JSONL metrics files.

Usage:
    python compare_backends.py keeper_metrics.jsonl
    python compare_backends.py keeper_metrics.jsonl --scenario CFG-02
    python compare_backends.py keeper_metrics.jsonl --source bench

Compares same scenarios across backends (e.g., CFG-02[default] vs CFG-02[rocks]).
"""
import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional


def load_jsonl(path: Path) -> List[dict]:
    """Load rows from JSONL file."""
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                try:
                    rows.append(json.loads(s))
                except json.JSONDecodeError:
                    continue
    return rows


def extract_base_scenario(scenario_id: str) -> str:
    """Extract base scenario name without backend/topology suffix.

    CFG-02[default|t3] -> CFG-02
    BND-01[rocks|t3] -> BND-01
    """
    match = re.match(r"^([A-Z]+-\d+)", scenario_id or "")
    if match:
        return match.group(1)
    if "[" in (scenario_id or ""):
        return scenario_id.split("[")[0]
    return scenario_id or "unknown"


def extract_metrics(
    rows: List[dict], source_filter: Optional[str] = None
) -> Dict[str, Dict[str, Dict[str, Dict[str, List[float]]]]]:
    """Extract metrics grouped by (base_scenario, backend, source, metric_name).

    Returns: {base_scenario: {backend: {source: {metric_name: [values]}}}}
    """
    data = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    )

    for r in rows:
        stage = r.get("stage", "")
        if stage not in ("summary", "post"):
            continue

        source = r.get("source", "unknown")
        if source_filter and source != source_filter:
            continue

        scenario_full = r.get("scenario", "")
        base_scenario = extract_base_scenario(scenario_full)
        backend = r.get("backend", "unknown")
        name = r.get("name", "")
        val = r.get("value")

        if val is None or not name:
            continue

        # Skip non-numeric or boolean-like metrics
        if name in ("bench_ran", "bench_has_latency", "zk_server_state", "zk_version"):
            continue

        try:
            data[base_scenario][backend][source][name].append(float(val))
        except (ValueError, TypeError):
            continue

    return data


def compute_mean(values: List[float]) -> float:
    """Compute mean of values."""
    if not values:
        return 0.0
    return sum(values) / len(values)


def fmt_value(v: float) -> str:
    """Format value for display."""
    if abs(v) >= 1000000:
        return f"{v/1000000:.2f}M"
    elif abs(v) >= 1000:
        return f"{v/1000:.2f}K"
    elif abs(v) < 0.01 and v != 0:
        return f"{v:.4f}"
    else:
        return f"{v:.2f}"


def compare_source(source: str, default_data: dict, rocks_data: dict) -> List[str]:
    """Compare default vs rocks for a single source within a scenario."""
    lines = []

    all_metrics = sorted(set(list(default_data.keys()) + list(rocks_data.keys())))

    if not all_metrics:
        return lines

    lines.append(f"    [{source}]")
    lines.append(f"      {'metric':<35} {'default':>14} {'rocks':>14} {'diff%':>10}")
    lines.append("      " + "-" * 75)

    for metric in all_metrics:
        d_vals = default_data.get(metric, [])
        r_vals = rocks_data.get(metric, [])

        d_mean = compute_mean(d_vals)
        r_mean = compute_mean(r_vals)

        # Skip if both are zero
        if d_mean == 0 and r_mean == 0:
            continue

        # Calculate percentage difference
        if d_mean != 0:
            pct_diff = (r_mean - d_mean) / abs(d_mean) * 100
        elif r_mean != 0:
            pct_diff = 100.0 if r_mean > 0 else -100.0
        else:
            pct_diff = 0.0

        lines.append(
            f"      {metric:<35} {fmt_value(d_mean):>14} {fmt_value(r_mean):>14} {pct_diff:>+9.1f}%"
        )

    return lines


def compare_scenario(
    base_scenario: str, default_data: dict, rocks_data: dict
) -> List[str]:
    """Compare default vs rocks for a single scenario across all sources."""
    lines = []

    all_sources = sorted(set(list(default_data.keys()) + list(rocks_data.keys())))

    # Prioritize bench source first
    source_order = ["bench", "mntr", "srvr", "dirs", "prom"]
    all_sources = sorted(
        all_sources, key=lambda s: source_order.index(s) if s in source_order else 99
    )

    lines.append(f"\n{'='*80}")
    lines.append(f"[{base_scenario}]")
    lines.append(f"{'='*80}")

    for source in all_sources:
        d_source = default_data.get(source, {})
        r_source = rocks_data.get(source, {})

        if not d_source and not r_source:
            continue

        src_lines = compare_source(source, d_source, r_source)
        if src_lines:
            lines.extend(src_lines)

    return lines


def compare_backends(data: Dict) -> int:
    """Compare default vs rocks for each scenario. Returns count of scenarios compared."""

    if not data:
        print("No metrics found")
        return 0

    print("=" * 80)
    print("KEEPER BACKEND COMPARISON: Default vs RocksDB")
    print("=" * 80)

    scenarios_compared = 0
    scenarios_skipped = []

    for base_scenario in sorted(data.keys()):
        scenario_data = data[base_scenario]
        default_data = scenario_data.get("default", {})
        rocks_data = scenario_data.get("rocks", {})

        if not default_data or not rocks_data:
            scenarios_skipped.append(base_scenario)
            continue

        lines = compare_scenario(base_scenario, default_data, rocks_data)
        print("\n".join(lines))
        scenarios_compared += 1

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Scenarios compared: {scenarios_compared}")

    if scenarios_skipped:
        print(
            f"Scenarios skipped (missing one backend): {', '.join(scenarios_skipped[:5])}"
        )
        if len(scenarios_skipped) > 5:
            print(f"  ... and {len(scenarios_skipped) - 5} more")

    print()
    return scenarios_compared


def main():
    ap = argparse.ArgumentParser(
        description="Compare Keeper backend performance per scenario"
    )
    ap.add_argument("jsonl", help="JSONL metrics file from CI")
    ap.add_argument(
        "--scenario", help="Filter to specific scenario prefix (e.g., CFG, BND)"
    )
    ap.add_argument(
        "--source", help="Filter to specific source (bench, mntr, srvr, dirs, prom)"
    )
    args = ap.parse_args()

    path = Path(args.jsonl)
    if not path.exists():
        print(f"File not found: {path}")
        return 1

    rows = load_jsonl(path)
    if not rows:
        print("No data in file")
        return 1

    # Filter by scenario if specified
    if args.scenario:
        rows = [
            r
            for r in rows
            if extract_base_scenario(r.get("scenario", "")).startswith(
                args.scenario.upper()
            )
        ]

    data = extract_metrics(rows, args.source)
    scenarios_compared = compare_backends(data)

    return 0 if scenarios_compared > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
