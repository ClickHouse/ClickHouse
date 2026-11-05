#!/usr/bin/env python3
"""Extract known failures from a SQLLogicTest complete-clickhouse report.json.

Usage:
    python3 generate_known_failures.py <report.json> [--output <file>]
    python3 generate_known_failures.py <report.json> --output-dir <directory>

Reads the report.json produced by the complete-clickhouse stage and prints
one failure ID per line (test_name:position), sorted. With ``--output-dir``,
it writes the path-based exclusion manifests used by the CI check.
"""

import argparse
import json
from pathlib import Path
import sys


def extract_failure_ids(report):
    """Extract sorted failure IDs from a report dict."""
    failures = []
    for test_name, test_data in report.get("tests", {}).items():
        for position, req in test_data.get("requests", {}).items():
            if req.get("status") == "error":
                failures.append(f"{test_name}:{position}")
    failures.sort()
    return failures


def exclusion_group(failure_id):
    parts = failure_id.split("/", 2)
    prefix = parts[0]
    if prefix == "random" and len(parts) > 1:
        random_group = f"random_{parts[1]}"
        if random_group in {
            "random_aggregates",
            "random_expr",
            "random_groupby",
            "random_select",
        }:
            return random_group
    if prefix in {"evidence", "index"}:
        return prefix
    return "select"


def write_exclusion_directory(failure_ids, output_dir):
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    groups = {
        key: []
        for key in (
            "evidence",
            "index",
            "random_aggregates",
            "random_expr",
            "random_groupby",
            "random_select",
            "select",
        )
    }
    for failure_id in failure_ids:
        groups[exclusion_group(failure_id)].append(failure_id)
    for key, values in groups.items():
        path = output_path / f"{key}.txt"
        path.write_text(
            "".join(f"{failure_id}\n" for failure_id in values),
            encoding="utf-8",
        )
        print(f"Wrote {len(values)} exclusions to {path}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Extract known failures from a SQLLogicTest report.json"
    )
    parser.add_argument("report_json", help="Path to report.json file")
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--output",
        "-o",
        help="Output file (default: stdout)",
        default=None,
    )
    output_group.add_argument(
        "--output-dir",
        help="Write the path-based exclusion manifests",
        default=None,
    )
    args = parser.parse_args()

    with open(args.report_json, "r", encoding="utf-8") as f:
        report = json.load(f)

    failure_ids = extract_failure_ids(report)

    if args.output_dir:
        write_exclusion_directory(failure_ids, args.output_dir)
        print(f"Extracted {len(failure_ids)} failures", file=sys.stderr)
        return

    out = open(args.output, "w", encoding="utf-8") if args.output else sys.stdout
    try:
        for fid in failure_ids:
            out.write(fid + "\n")
    finally:
        if out is not sys.stdout:
            out.close()

    print(f"Extracted {len(failure_ids)} failures", file=sys.stderr)


if __name__ == "__main__":
    main()
