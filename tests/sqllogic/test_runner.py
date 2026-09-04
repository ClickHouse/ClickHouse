#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import enum
import io
import json
import logging
import os

import test_parser
from connection import execute_request
from exceptions import (
    DataResultDiffer,
    Error,
    ProgramError,
    QueryExecutionError,
    QuerySuccess,
    SchemeResultDiffer,
    StatementExecutionError,
    StatementSuccess,
)

logger = logging.getLogger("parser")
logger.setLevel(logging.DEBUG)


def _list_files(path):
    logger.debug("list files in %s, type %s", path, type(path))

    if not isinstance(path, str):
        raise ProgramError("NotImplemented")

    if os.path.isfile(path):
        yield path
    else:
        with os.scandir(path) as it:
            for entry in it:
                yield from _list_files(entry.path)


def _filter_files(suffix, files):
    yield from (path for path in files if path.endswith(suffix))


class RequestType(str, enum.Enum):
    statement = enum.auto()
    query = enum.auto()


class Status(str, enum.Enum):
    success = "success"
    error = "error"


class TestStatus:
    def __init__(self):
        self.name = None
        self.status = None
        self.file = None
        self.position = None
        self.request_type = None
        self.request = None
        self.reason = None

    def get_map(self):
        return {
            "status": self.status.name.lower(),
            # "file": self.file,
            "position": self.position,
            "request_type": self.request_type.name.lower(),
            "request": self.request,
            "reason": self.reason,
        }

    @staticmethod
    def __from_error(err):
        if isinstance(err, Error):
            result = TestStatus()
            result.name = err.test_name
            result.file = err.test_file
            result.position = err.test_pos
            result.request = err.request
            result.reason = err.reason
            return result
        raise ProgramError("NotImplemented")

    @staticmethod
    def from_exception(ex):
        result = TestStatus.__from_error(ex)

        if isinstance(ex, StatementSuccess):
            result.status = Status.success
            result.request_type = RequestType.statement
        elif isinstance(ex, StatementExecutionError):
            result.status = Status.error
            result.request_type = RequestType.statement
        elif isinstance(ex, QuerySuccess):
            result.status = Status.success
            result.request_type = RequestType.query
        elif isinstance(ex, QueryExecutionError):
            result.status = Status.error
            result.request_type = RequestType.query
        elif isinstance(ex, SchemeResultDiffer):
            result.status = Status.error
            result.request_type = RequestType.query
        elif isinstance(ex, DataResultDiffer):
            result.status = Status.error
            result.request_type = RequestType.query
        else:
            raise ProgramError("NotImplemented", parent=ex)

        return result


class SimpleStats:
    def __init__(self, general=None):
        self._general = general
        self._success = 0
        self._fail = 0

    @property
    def all(self):
        return self._success + self.fail

    @property
    def success(self):
        return self._success

    @success.setter
    def success(self, value):
        if self._general is not None:
            self._general.success += value - self._success
        self._success = value

    @property
    def fail(self):
        return self._fail

    @fail.setter
    def fail(self, value):
        if self._general is not None:
            self._general.fail += value - self._fail
        self._fail = value

    def __repr__(self):
        return str(self.get_map())

    def update(self, status):
        if not isinstance(status, TestStatus):
            raise ProgramError("NotImplemented")

        if status.status == Status.error:
            self.fail += 1
        else:
            self.success += 1

    def get_map(self):
        result = {}
        result["success"] = self.success
        result["fail"] = self.fail
        return result

    def combine_with(self, right):
        if not isinstance(right, SimpleStats):
            raise ProgramError("NotImplemented")
        self.success += right.success
        self.fail += right.fail


class Stats:
    def __init__(self):
        self.total = SimpleStats()
        self.statements = SimpleStats(self.total)
        self.queries = SimpleStats(self.total)

    def __repr__(self):
        return str(self.get_map())

    def update(self, status):
        if not isinstance(status, TestStatus):
            raise ProgramError("NotImplemented")

        if status.request_type == RequestType.query:
            choose = self.queries
        else:
            choose = self.statements
        choose.update(status)

    def get_map(self):
        result = {}
        result["statements"] = self.statements.get_map()
        result["queries"] = self.queries.get_map()
        result["total"] = self.total.get_map()
        return result

    def combine_with(self, right):
        if not isinstance(right, Stats):
            raise ProgramError("NotImplemented")
        self.statements.combine_with(right.statements)
        self.queries.combine_with(right.queries)


class OneReport:
    def __init__(self, test_name, test_file):
        self.test_name = test_name
        self.test_file = test_file
        self.stats = Stats()
        self.requests = {}

    def update(self, status):
        if not isinstance(status, TestStatus):
            raise ProgramError("NotImplemented")

        self.stats.update(status)
        self.requests[status.position] = status

    def __repr__(self):
        return str(self.get_map())

    def get_map(self):
        result = {}
        result["test_name"] = self.test_name
        result["test_file"] = self.test_file
        result["stats"] = self.stats.get_map()
        result["requests"] = {}
        requests = result["requests"]
        for pos, status in self.requests.items():
            requests[pos] = status.get_map()
        return result


class Report:
    def __init__(self, dbms_name, input_dir=None):
        self.dbms_name = dbms_name
        self.stats = Stats()
        self.tests = {}
        self.input_dir = input_dir
        self.output_dir = None

    def update(self, status):
        if not isinstance(status, TestStatus):
            raise ProgramError("NotImplemented")

        self.stats.update(status)
        self.__get_file_report(status).update(status)

    def __get_file_report(self, status):
        if status.name not in self.tests:
            self.tests[status.name] = OneReport(status.name, status.file)
        return self.tests[status.name]

    def __repr__(self):
        return str(self.get_map())

    def assign_result_dir(self, res_dir):
        self.output_dir = res_dir

    def get_map(self):
        result = {}
        result["dbms_name"] = self.dbms_name
        result["stats"] = self.stats.get_map()
        result["input_dir"] = self.input_dir
        if self.input_dir is not None:
            result["input_dir"] = self.input_dir
        if self.output_dir is not None:
            result["output_dir"] = self.output_dir
        result["tests"] = {}
        tests = result["tests"]
        for test_name, one_report in self.tests.items():
            tests.update({test_name: one_report.get_map()})
        return result

    def combine_with(self, right):
        if not isinstance(right, Report):
            raise ProgramError("NotImplemented")

        if self.dbms_name != right.dbms_name:
            raise ProgramError("reports are attached to the different databases")

        if self.input_dir is None or right.input_dir is None:
            raise ProgramError("can't compare input dirs")

        if self.input_dir != right.input_dir:
            raise ProgramError(
                "can't combine reports, they are attached to the different input dirs"
            )

        for test_name in right.tests.keys():
            if test_name in self.tests:
                raise ProgramError(
                    f"can't combine reports, they have intersect tests, {test_name}"
                )

        self.tests.update(right.tests)
        self.stats.combine_with(right.stats)
        return self

    def write_report(self, report_dir):
        report_path = os.path.join(report_dir, "report.json")
        logger.info("create file %s", report_path)
        with open(report_path, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(self.get_map(), indent=4))


class TestRunner:
    def __init__(
        self,
        connection,
        verify_mode=None,
        skip_request_types=None,
        stop_at_statement_error=None,
    ):
        self.connection = connection
        self.verify = False if verify_mode is None else verify_mode
        self.skip_request_types = []
        if skip_request_types is not None:
            for req_type in skip_request_types:
                self.with_skip(req_type)
        self.stop_at_statement_error = (
            False if stop_at_statement_error is None else stop_at_statement_error
        )

        self.dbms_name = connection.DBMS_NAME
        self.report = None
        self.results = None
        self._input_dir = None

    def with_verify_mode(self):
        self.verify = True
        return self

    def with_completion_mode(self):
        self.verify = False
        return self

    def with_skip(self, type_request):
        if type_request == RequestType.query:
            self.skip_request_types.append(test_parser.BlockType.query)
        if type_request == RequestType.statement:
            self.skip_request_types.append(test_parser.BlockType.statement)

    def __statuses(self, parser, out_stream):
        skip_rest = False

        for block in parser.test_blocks():
            test_file = parser.get_test_file()
            test_name = parser.get_test_name()
            position = block.get_pos()
            name_pos = f"{test_name}:{position}"

            clogger = logging.getLogger(f"parser at {name_pos}")

            if skip_rest:
                clogger.debug("Skip rest blocks")
                block.dump_to(out_stream)
                continue

            if block.get_block_type() == test_parser.BlockType.comments:
                clogger.debug("Skip comment block")
                block.dump_to(out_stream)
                continue

            if block.get_block_type() == test_parser.BlockType.control:
                clogger.debug("Skip control block %s", name_pos)
                block.dump_to(out_stream)
                continue

            clogger.debug("Request <%s>", block.get_request())

            cond_lines = block.get_conditions()
            if not test_parser.check_conditions(cond_lines, self.dbms_name):
                clogger.debug("Conditionally skip block for %s", self.dbms_name)
                block.dump_to(out_stream)
                continue

            request = block.get_request()

            if block.get_block_type() in self.skip_request_types:
                clogger.debug("Runtime skip block for %s", self.dbms_name)
                block.dump_to(out_stream)
                continue

            exec_res = execute_request(request, self.connection)

            if block.get_block_type() == test_parser.BlockType.statement:
                try:
                    clogger.debug("this is statement")
                    if block.expected_error():
                        clogger.debug("error is expected")
                        if not exec_res.has_exception():
                            raise StatementExecutionError(
                                "statement request did not fail as expected"
                            )
                    else:
                        clogger.debug("ok is expected")
                        if exec_res.has_exception():
                            raise StatementExecutionError(
                                "statement failed with exception",
                                parent=exec_res.get_exception(),
                            )
                    raise StatementSuccess()
                except StatementSuccess as ok:
                    clogger.debug("statement is ok")
                    ok.set_details(
                        file=test_file, name=test_name, pos=position, request=request
                    )
                    block.dump_to(out_stream)
                    yield TestStatus.from_exception(ok)
                except StatementExecutionError as err:
                    err.set_details(
                        file=test_file, name=test_name, pos=position, request=request
                    )
                    clogger.critical("Unable to execute statement, %s", err.reason)
                    block.dump_to(out_stream)
                    if self.stop_at_statement_error:
                        clogger.critical("Will skip the rest of the file")
                        skip_rest = True
                    yield TestStatus.from_exception(err)

            if block.get_block_type() == test_parser.BlockType.query:
                try:
                    clogger.debug("this is query")
                    expected_error = block.expected_error()
                    if expected_error:
                        clogger.debug("error is expected %s", expected_error)
                        if exec_res.has_exception():
                            e = exec_res.get_exception()
                            clogger.debug("had error %s", e)
                            message = str(e).lower()
                            if expected_error not in message:
                                clogger.debug("errors differed")
                                raise QueryExecutionError(
                                    "query is expected to fail with different error",
                                    details=f"expected error: {expected_error}",
                                    parent=exec_res.get_exception(),
                                )
                            clogger.debug("errors matched")
                            raise QuerySuccess()
                        clogger.debug("missed error")
                        raise QueryExecutionError(
                            "query is expected to fail with error",
                            details=f"expected error: {expected_error}",
                        )
                    clogger.debug("success is expected")
                    if exec_res.has_exception():
                        clogger.debug("had error")
                        if self.verify:
                            clogger.debug("verify mode")
                            canonic = test_parser.QueryResult.parse_it(
                                block.get_result(), 10
                            )
                            exception = QueryExecutionError(
                                "query execution failed with an exception",
                                parent=exec_res.get_exception(),
                            )
                            actual = test_parser.QueryResult.as_exception(exception)
                            test_parser.QueryResult.assert_eq(canonic, actual)
                            block.with_result(actual)
                            raise QuerySuccess()
                        clogger.debug("completion mode")
                        raise QueryExecutionError(
                            "query execution failed with an exception",
                            parent=exec_res.get_exception(),
                        )

                    canonic_types = block.get_types()
                    clogger.debug("canonic types %s", canonic_types)

                    if len(exec_res.get_result()) > 0:
                        actual_columns_count = len(exec_res.get_result()[0])
                        canonic_columns_count = len(canonic_types)
                        if canonic_columns_count != actual_columns_count:
                            raise SchemeResultDiffer(
                                "canonic and actual columns count differ",
                                details=f"expected columns {canonic_columns_count}, "
                                f"actual columns {actual_columns_count}",
                            )

                    actual = test_parser.QueryResult.make_it(
                        exec_res.get_result(), canonic_types, block.get_sort_mode(), 10
                    )

                    if self.verify:
                        clogger.debug("verify mode")
                        canonic = test_parser.QueryResult.parse_it(
                            block.get_result(), 10
                        )
                        test_parser.QueryResult.assert_eq(canonic, actual)

                    block.with_result(actual)
                    raise QuerySuccess()

                except QuerySuccess as ok:
                    ok.set_details(
                        file=test_file, name=test_name, pos=position, request=request
                    )
                    clogger.debug("query ok")
                    block.dump_to(out_stream)
                    yield TestStatus.from_exception(ok)
                except Error as err:
                    err.set_details(
                        file=test_file, name=test_name, pos=position, request=request
                    )
                    clogger.warning(
                        "Query has failed with exception: %s",
                        err.reason,
                    )
                    block.with_result(test_parser.QueryResult.as_exception(err))
                    block.dump_to(out_stream)
                    yield TestStatus.from_exception(err)

    def run_one_test(self, stream, test_name, test_file):
        if self._input_dir is not None:
            if not test_file.startswith(self._input_dir):
                raise ProgramError(
                    f"that runner instance is attached to tests in dir {self._input_dir}"
                    f", can't run with file {test_file}"
                )
        else:
            self._input_dir = os.path.dirname(test_file)

        if self.report is None:
            self.report = Report(self.dbms_name, self._input_dir)

        if self.results is None:
            self.results = {}

        if self.dbms_name == "ClickHouse" and test_name in [
            "test/select5.test",
            "test/evidence/slt_lang_createtrigger.test",
            "test/evidence/slt_lang_replace.test",
            "test/evidence/slt_lang_droptrigger.test",
        ]:
            logger.info("Let's skip test %s for ClickHouse", test_name)
            return

        with self.connection.with_one_test_scope():
            out_stream = io.StringIO()
            self.results[test_name] = out_stream

            parser = test_parser.TestFileParser(
                stream, test_name, test_file, self.dbms_name
            )
            for status in self.__statuses(parser, out_stream):
                self.report.update(status)

    def _assert_input_dir(self, input_dir):
        if self._input_dir is not None:
            if self._input_dir != input_dir:
                raise ProgramError(
                    f"that runner instance is attached to tests in dir {self._input_dir}"
                    f", can't run with {input_dir}"
                )

    def run_all_tests_from_file(self, test_file, input_dir=None):
        self._assert_input_dir(input_dir)
        self._input_dir = input_dir
        if self._input_dir is None:
            self._input_dir = os.path.dirname(test_file)

        test_name = os.path.relpath(test_file, start=self._input_dir)
        logger.debug("open file %s", test_name)
        with open(test_file, "r", encoding="utf-8") as stream:
            self.run_one_test(stream, test_name, test_file)

    def run_all_tests_from_dir(self, input_dir):
        self._assert_input_dir(input_dir)
        self._input_dir = input_dir
        for file_path in TestRunner.list_tests(self._input_dir):
            self.run_all_tests_from_file(file_path, self._input_dir)

    def write_results_to_dir(self, dir_path):
        if not os.path.isdir(dir_path):
            raise NotADirectoryError(dir_path)

        self.report.assign_result_dir(dir_path)

        for test_name, stream in self.results.items():
            test_file = os.path.join(dir_path, test_name)
            logger.info("create file %s", test_file)
            result_dir = os.path.dirname(test_file)
            os.makedirs(result_dir, exist_ok=True)
            with open(test_file, "w", encoding="utf-8") as output:
                output.write(stream.getvalue())

    def write_report(self, report_dir):
        self.report.write_report(report_dir)

    @staticmethod
    def list_tests(input_dir):
        yield from _filter_files(".test", _list_files(input_dir))
