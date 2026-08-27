#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import enum
import json
import logging
import multiprocessing
import os
from functools import reduce

from deepdiff import DeepDiff  # pylint:disable=import-error; for style check

from connection import Engines, default_clickhouse_odbc_conn_str, setup_connection
from test_runner import RequestType, Status, TestRunner

LEVEL_NAMES = [  # pylint:disable-next=protected-access
    l.lower() for l, n in logging._nameToLevel.items() if n != logging.NOTSET
]


def setup_logger(args):
    logging.getLogger().setLevel(logging.NOTSET)
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(filename)s %(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.log_file:
        file_handler = logging.FileHandler(args.log_file)
        file_handler.setLevel(args.log_level.upper())
        file_handler.setFormatter(formatter)
        logging.getLogger().addHandler(file_handler)
    else:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)
        logging.getLogger().addHandler(stream_handler)


def __write_check_status(status_row, out_dir):
    if len(status_row) > 140:
        status_row = status_row[0:135] + "..."
    check_status_path = os.path.join(out_dir, "check_status.tsv")
    with open(check_status_path, "a", encoding="utf-8") as stream:
        writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
        writer.writerow(status_row)


class TestNameGranularity(str, enum.Enum):
    file = enum.auto()
    request = enum.auto()


def __write_test_result(
    reports,
    out_dir,
    mode_name,
    granularity=TestNameGranularity.request,
    only_errors=None,
):
    all_stages = reports.keys()
    test_results_path = os.path.join(out_dir, "test_results.tsv")
    with open(test_results_path, "a", encoding="utf-8") as stream:
        writer = csv.writer(stream, delimiter="\t", lineterminator="\n")
        for stage in all_stages:
            report = reports[stage]
            for test_report in report.tests.values():
                test_name_prefix = (
                    f"sqllogic::{mode_name}::{stage}::{test_report.test_name}"
                )

                for request_status in test_report.requests.values():
                    if request_status.status == Status.error or not only_errors:
                        test_name = test_name_prefix
                        if granularity == TestNameGranularity.request:
                            test_name += f"::{request_status.position}"

                        test_status = "success"
                        if request_status.status == Status.error:
                            test_status = "FAIL"

                        log_row = (
                            f"position: {request_status.position}"
                            f", type: {request_status.request_type.name.lower()}"
                            f", request: '{request_status.request}'"
                        )
                        if request_status.status == Status.error:
                            log_row += f", reason: '{request_status.reason}'"

                        writer.writerow(
                            [
                                test_name,
                                test_status,
                                0,
                                log_row,
                            ]
                        )


def statements_report(reports, out_dir, mode_name):
    __write_test_result(
        reports,
        out_dir,
        mode_name,
        granularity=TestNameGranularity.file,
        only_errors=True,
    )

    failed_stages = []
    for stage, report in reports.items():
        if report.stats.total.fail > 0:
            failed_stages.append(stage)

    if len(failed_stages) == 0:
        status_row = [
            "success",
            f"All tests from {mode_name} are successful",
        ]
        __write_check_status(status_row, out_dir)
        return

    stage = max(failed_stages, key=lambda x: reports[x].stats.total.fail)
    stats = reports[stage].stats
    status_row = [
        "error",
        f"{stats.total.fail}/{stats.total.all} tests failed at {mode_name}::{stage}",
    ]
    __write_check_status(status_row, out_dir)


def _child_process(setup_kwargs, runner_kwargs, input_dir, output_dir, test):
    with setup_connection(**setup_kwargs) as connection:
        with connection.with_test_database_scope():
            runner = TestRunner(connection, **runner_kwargs)
            runner.run_all_tests_from_file(test, input_dir)
            runner.write_results_to_dir(output_dir)
            return runner.report


def run_all_tests_in_parallel(setup_kwargs, runner_kwargs, input_dir, output_dir):
    process_count = max(1, os.cpu_count() - 2)
    with multiprocessing.Pool(process_count) as pool:
        async_results = [
            pool.apply_async(
                _child_process,
                args=(
                    setup_kwargs,
                    runner_kwargs,
                    input_dir,
                    output_dir,
                    test,
                ),
            )
            for test in TestRunner.list_tests(input_dir)
        ]
        reports = [ar.get() for ar in async_results]

    report = reduce(lambda x, y: x.combine_with(y), reports)
    report.write_report(output_dir)
    return report


def as_kwargs(**kwargs):
    return kwargs


def mode_check_statements(parser):
    parser.add_argument("--input-dir", metavar="DIR", required=True)
    parser.add_argument("--out-dir", metavar="DIR", required=True)

    def calle(args):
        input_dir = os.path.realpath(args.input_dir)
        out_dir = os.path.realpath(args.out_dir)

        if not os.path.exists(input_dir):
            raise FileNotFoundError(
                input_dir, f"check statements: no such file or directory {input_dir}"
            )

        if not os.path.isdir(input_dir):
            raise NotADirectoryError(
                input_dir, f"check statements:: not a dir {input_dir}"
            )

        reports = {}

        out_stages_dir = os.path.join(out_dir, f"{args.mode}-stages")

        complete_sqlite_dir = os.path.join(out_stages_dir, "statements-sqlite")
        os.makedirs(complete_sqlite_dir, exist_ok=True)

        reports["statements-sqlite"] = run_all_tests_in_parallel(
            setup_kwargs=as_kwargs(
                engine=Engines.SQLITE,
            ),
            runner_kwargs=as_kwargs(
                verify_mode=False,
                skip_request_types=[RequestType.query],
                stop_at_statement_error=True,
            ),
            input_dir=input_dir,
            output_dir=complete_sqlite_dir,
        )

        verify_clickhouse_dir = os.path.join(out_stages_dir, "verify-clickhouse")
        os.makedirs(verify_clickhouse_dir, exist_ok=True)

        reports["verify-clickhouse"] = run_all_tests_in_parallel(
            setup_kwargs=as_kwargs(
                engine=Engines.ODBC,
                conn_str=default_clickhouse_odbc_conn_str(),
            ),
            runner_kwargs=as_kwargs(
                verify_mode=True,
                skip_request_types=[RequestType.query],
                stop_at_statement_error=True,
            ),
            input_dir=complete_sqlite_dir,
            output_dir=verify_clickhouse_dir,
        )

        statements_report(reports, out_dir, args.mode)

    parser.set_defaults(func=calle)


def mode_check_complete(parser):
    parser.add_argument("--input-dir", metavar="DIR", required=True)
    parser.add_argument("--out-dir", metavar="DIR", required=True)

    def calle(args):
        input_dir = os.path.realpath(args.input_dir)
        out_dir = os.path.realpath(args.out_dir)

        if not os.path.exists(input_dir):
            raise FileNotFoundError(
                input_dir, f"check statements: no such file or directory {input_dir}"
            )

        if not os.path.isdir(input_dir):
            raise NotADirectoryError(
                input_dir, f"check statements:: not a dir {input_dir}"
            )

        reports = {}

        out_stages_dir = os.path.join(out_dir, f"{args.mode}-stages")

        complete_sqlite_dir = os.path.join(out_stages_dir, "complete-sqlite")
        os.makedirs(complete_sqlite_dir, exist_ok=True)

        reports["complete-sqlite"] = run_all_tests_in_parallel(
            setup_kwargs=as_kwargs(
                engine=Engines.SQLITE,
            ),
            runner_kwargs=as_kwargs(
                verify_mode=False,
                stop_at_statement_error=True,
            ),
            input_dir=input_dir,
            output_dir=complete_sqlite_dir,
        )

        verify_clickhouse_dir = os.path.join(out_stages_dir, "complete-clickhouse")
        os.makedirs(verify_clickhouse_dir, exist_ok=True)

        reports["complete-clickhouse"] = run_all_tests_in_parallel(
            setup_kwargs=as_kwargs(
                engine=Engines.ODBC,
                conn_str=default_clickhouse_odbc_conn_str(),
            ),
            runner_kwargs=as_kwargs(
                verify_mode=True,
                stop_at_statement_error=True,
            ),
            input_dir=complete_sqlite_dir,
            output_dir=verify_clickhouse_dir,
        )

        statements_report(reports, out_dir, args.mode)

    parser.set_defaults(func=calle)


def make_actual_report(reports):
    return {stage: report.get_map() for stage, report in reports.items()}


def write_actual_report(actual, out_dir):
    with open(os.path.join(out_dir, "actual_report.json"), "w", encoding="utf-8") as f:
        f.write(json.dumps(actual))


def read_canonic_report(input_dir):
    file = os.path.join(input_dir, "canonic_report.json")
    if not os.path.exists(file):
        return {}

    with open(
        os.path.join(input_dir, "canonic_report.json"), "r", encoding="utf-8"
    ) as f:
        data = f.read()
    return json.loads(data)


def write_canonic_report(canonic, out_dir):
    with open(os.path.join(out_dir, "canonic_report.json"), "w", encoding="utf-8") as f:
        f.write(json.dumps(canonic))


def self_test_report(reports, input_dir, out_dir, mode_name):
    actual = make_actual_report(reports)
    write_actual_report(actual, out_dir)

    canonic = read_canonic_report(input_dir)
    write_canonic_report(canonic, out_dir)

    status_row = [
        "success",
        f"All statements from {mode_name} are successful",
    ]

    failed_stages = {}

    for stage, actual_report in actual.items():
        actual_stats = actual_report["stats"]

        if stage not in canonic:
            failed_stages[stage] = actual_stats.items()
            continue

        canonic_report = canonic[stage]
        canonic_stats = canonic_report["stats"]

        logging.debug("stage: %s, canonic: %s", stage, canonic_stats)
        logging.debug("stage: %s, actual: %s", stage, actual_stats)

        diff = DeepDiff(actual_stats, canonic_stats)
        if len(diff):
            failed_stages[stage] = diff
            logging.error("diff: %s", diff)
        else:
            logging.debug("diff: %s", diff)

    all_stages = actual.keys()
    if len(failed_stages) > 0:
        description = f"Failed {len(failed_stages)}/{len(all_stages)} from {mode_name}, stages: {','.join(failed_stages)}"
        status_row = ["error", description]

    __write_check_status(status_row, out_dir)


def mode_self_test(parser):
    parser.add_argument("--self-test-dir", metavar="DIR", required=True)
    parser.add_argument("--out-dir", metavar="DIR", required=True)

    def calle(args):
        self_test_dir = os.path.realpath(args.self_test_dir)
        if not os.path.exists(self_test_dir):
            raise FileNotFoundError(
                self_test_dir, f"self test: no such file or directory {self_test_dir}"
            )
        if not os.path.isdir(self_test_dir):
            raise NotADirectoryError(
                self_test_dir, f"self test: not a dir {self_test_dir}"
            )
        logging.debug("self test dir is: %s", self_test_dir)

        out_dir = os.path.realpath(args.out_dir)
        if not os.path.exists(out_dir):
            raise FileNotFoundError(out_dir, f"self test: dir not found {out_dir}")
        if not os.path.isdir(out_dir):
            raise NotADirectoryError(out_dir, f"self test: not a dir {out_dir}")

        reports = {}

        out_stages_dir = os.path.join(out_dir, f"{args.mode}-stages")

        out_dir_sqlite_complete = os.path.join(out_stages_dir, "sqlite-complete")
        os.makedirs(out_dir_sqlite_complete, exist_ok=True)
        with setup_connection(Engines.SQLITE) as sqlite:
            runner = TestRunner(sqlite)
            runner.run_all_tests_from_dir(self_test_dir)
            runner.write_results_to_dir(out_dir_sqlite_complete)
            runner.write_report(out_dir_sqlite_complete)
            reports["sqlite-complete"] = runner.report

        out_dir_sqlite_vs_sqlite = os.path.join(out_stages_dir, "sqlite-vs-sqlite")
        os.makedirs(out_dir_sqlite_vs_sqlite, exist_ok=True)
        with setup_connection(Engines.SQLITE) as sqlite:
            runner = TestRunner(sqlite)
            runner.with_verify_mode()
            runner.run_all_tests_from_dir(out_dir_sqlite_complete)
            runner.write_results_to_dir(out_dir_sqlite_vs_sqlite)
            runner.write_report(out_dir_sqlite_vs_sqlite)
            reports["sqlite-vs-sqlite"] = runner.report

        out_dir_clickhouse_complete = os.path.join(
            out_stages_dir, "clickhouse-complete"
        )
        os.makedirs(out_dir_clickhouse_complete, exist_ok=True)
        with setup_connection(
            Engines.ODBC, default_clickhouse_odbc_conn_str()
        ) as clickhouse:
            runner = TestRunner(clickhouse)
            runner.run_all_tests_from_dir(self_test_dir)
            runner.write_results_to_dir(out_dir_clickhouse_complete)
            runner.write_report(out_dir_clickhouse_complete)
            reports["clickhouse-complete"] = runner.report

        out_dir_clickhouse_vs_clickhouse = os.path.join(
            out_stages_dir, "clickhouse-vs-clickhouse"
        )
        os.makedirs(out_dir_clickhouse_vs_clickhouse, exist_ok=True)
        with setup_connection(
            Engines.ODBC, default_clickhouse_odbc_conn_str()
        ) as clickhouse:
            runner = TestRunner(clickhouse)
            runner.with_verify_mode()
            runner.run_all_tests_from_dir(out_dir_clickhouse_complete)
            runner.write_results_to_dir(out_dir_clickhouse_vs_clickhouse)
            runner.write_report(os.path.join(out_dir_clickhouse_vs_clickhouse))
            reports["clickhouse-vs-clickhouse"] = runner.report

        out_dir_sqlite_vs_clickhouse = os.path.join(
            out_stages_dir, "sqlite-vs-clickhouse"
        )
        os.makedirs(out_dir_sqlite_vs_clickhouse, exist_ok=True)

        reports["sqlite-vs-clickhouse"] = run_all_tests_in_parallel(
            setup_kwargs=as_kwargs(
                engine=Engines.ODBC,
                conn_str=default_clickhouse_odbc_conn_str(),
            ),
            runner_kwargs=as_kwargs(
                verify_mode=True,
            ),
            input_dir=out_dir_sqlite_complete,
            output_dir=out_dir_sqlite_vs_clickhouse,
        )

        self_test_report(reports, self_test_dir, out_dir, args.mode)

    parser.set_defaults(func=calle)


def parse_args():
    parser = argparse.ArgumentParser(
        description="This script runs sqllogic tests over database."
    )

    parser.add_argument("--log-file", help="write logs to the file", metavar="FILE")
    parser.add_argument(
        "--log-level",
        help="define the log level for log file",
        metavar="level",
        choices=LEVEL_NAMES,
        default="debug",
    )

    subparsers = parser.add_subparsers(dest="mode")
    mode_check_complete(
        subparsers.add_parser(
            "complete-test",
            help="Run all tests. Check that all statements and queries are passed",
        )
    )
    mode_check_statements(
        subparsers.add_parser(
            "statements-test",
            help="Run all tests. Check that all statements are passed",
        )
    )
    mode_self_test(
        subparsers.add_parser(
            "self-test",
            help="Run all tests. Check that all statements are passed",
        )
    )
    args = parser.parse_args()
    if args.mode is None:
        parser.print_help()
    return args


def main():
    args = parse_args()
    setup_logger(args)
    if args.mode is not None:
        args.func(args)


if __name__ == "__main__":
    main()
