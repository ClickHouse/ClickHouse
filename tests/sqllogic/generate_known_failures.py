#!/usr/bin/env python3
"""Extract known failures from a SQLLogicTest complete-clickhouse report.json.

Usage:
    python3 generate_known_failures.py <report.json> [--output <file>]

Reads the report.json produced by the complete-clickhouse stage and prints
one failure ID per line (test_name:position), sorted. The output can be
redirected to tests/sqllogic/known_failures.txt.
"""

import argparse
import json
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


def main():
    parser = argparse.ArgumentParser(
        description="Extract known failures from a SQLLogicTest report.json"
    )
    parser.add_argument("report_json", help="Path to report.json file")
    parser.add_argument(
        "--output",
        "-o",
        help="Output file (default: stdout)",
        default=None,
    )
    args = parser.parse_args()

    with open(args.report_json, "r", encoding="utf-8") as f:
        report = json.load(f)

    failure_ids = extract_failure_ids(report)

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
