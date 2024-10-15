#!/usr/bin/env python3

import argparse
import csv
import logging
import os


# TODO: add typing and log files to the fourth column, think about launching
# everything from the python and not bash
def process_result(result_folder):
    status = "success"
    description = ""
    test_results = []
    checks = (
        # "duplicate includes",  # disabled in favor of clang-tidy
        "shellcheck",
        "style",
        "pylint",
        "isort",
        "black",
        "flake8",
        "mypy",
        "typos",
        "whitespaces",
        "workflows",
        "submodules",
        "docs spelling",
    )

    for name in checks:
        out_file = name.replace(" ", "_") + "_output.txt"
        full_path = os.path.join(result_folder, out_file)
        if not os.path.exists(full_path):
            test_results.append((f"Check {name}", "SKIPPED"))
        elif os.stat(full_path).st_size != 0:
            with open(full_path, "r") as file:
                lines = file.readlines()
                if len(lines) > 100:
                    lines = lines[:100] + ["====TRIMMED===="]
                content = "\n".join(lines)
            description += f"Check {name} failed. "
            test_results.append((f"Check {name}", "FAIL", None, content))
            status = "failure"
        else:
            test_results.append((f"Check {name}", "OK"))

    if not description:
        description += "Style check success"

    assert test_results, "No single style-check output found"

    return status, description, test_results


def write_results(results_file, status_file, results, status):
    with open(results_file, "w", encoding="utf-8") as f:
        out = csv.writer(f, delimiter="\t")
        out.writerows(results)
    with open(status_file, "w", encoding="utf-8") as f:
        out = csv.writer(f, delimiter="\t")
        out.writerow(status)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(
        description="ClickHouse script for parsing results of style check"
    )
    default_dir = "/test_output"
    parser.add_argument("--in-results-dir", default=default_dir)
    parser.add_argument("--out-results-file", default=f"{default_dir}/test_results.tsv")
    parser.add_argument("--out-status-file", default=f"{default_dir}/check_status.tsv")
    args = parser.parse_args()

    state, description, test_results = process_result(args.in_results_dir)
    logging.info("Result parsed")
    status = (state, description)
    write_results(args.out_results_file, args.out_status_file, test_results, status)
    logging.info("Result written")
