#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from enum import Enum
from functools import reduce
from hashlib import md5
from itertools import chain

# pylint:disable=import-error; for style check
import sqlglot
from sqlglot.expressions import ColumnDef, PrimaryKeyColumnConstraint

from exceptions import (
    DataResultDiffer,
    Error,
    ErrorWithParent,
    ProgramError,
    QueryExecutionError,
)

# pylint:enable=import-error; for style check


logger = logging.getLogger("parser")
logger.setLevel(logging.DEBUG)

CONDITION_SKIP = "skipif"
CONDITION_ONLY = "onlyif"


# TODO replace assertions with raise exception
class TestFileFormatException(Error):
    pass


class FileAndPos:
    def __init__(self, file=None, pos=None):
        self.file = file
        self.pos = pos

    def __str__(self):
        return f"{self.file}:{self.pos}"


def check_conditions(conditions, dbms_name):
    rules = {}
    for rec in conditions:
        key, val = rec
        if key not in conditions:
            rules[key] = []
        rules[key].append(val)
    if CONDITION_SKIP in rules:
        if dbms_name in rules[CONDITION_SKIP]:
            return False
    if CONDITION_ONLY in rules:
        if dbms_name not in rules[CONDITION_ONLY]:
            return False
    return True


class BlockType(Enum):
    comments = 1
    control = 2
    statement = 3
    query = 4


COMMENT_TOKENS = ["#"]
RESULT_SEPARATION_LINE = "----"
CONTROL_TOKENS = ["halt", "hash-threshold"]

CONDITIONS_TOKENS = [CONDITION_SKIP, CONDITION_ONLY]
STATEMENT_TOKEN = "statement"
QUERY_TOKEN = "query"


ACCEPTABLE_TYPES = {type(""): "T", type(1): "I", type(0.001): "R"}


def _is_comment_line(tokens):
    return tokens and tokens[0][0] in COMMENT_TOKENS


def _is_separation_line(tokens):
    return tokens and tokens[0] == RESULT_SEPARATION_LINE


def _is_control_line(tokens):
    return tokens and tokens[0] in CONTROL_TOKENS


def _is_conditional_line(tokens):
    return tokens and tokens[0] in CONDITIONS_TOKENS


def _is_statement_line(tokens):
    return tokens and tokens[0] == STATEMENT_TOKEN


def _is_query_line(tokens):
    return tokens and tokens[0] == QUERY_TOKEN


class FileBlockBase:
    def __init__(self, parser, start, end):
        self._parser = parser
        self._start = start
        self._end = end

    def get_block_type(self):
        pass

    def get_pos(self):
        return self._start + 1

    @staticmethod
    def __parse_request(test_file, start, end):
        request_end = start
        while request_end < end:
            tokens = test_file.get_tokens(request_end)
            if not tokens or _is_separation_line(tokens):
                break
            request_end += 1
        request = test_file.get_tokens_from_lines(start, request_end)
        logger.debug("slice request %s:%s end %s", start, request_end, end)
        return " ".join(request), request_end

    @staticmethod
    def __parse_result(test_file, start, end):
        result_end = start
        while result_end < end:
            tokens = test_file.get_tokens(result_end)
            if not tokens:
                break
            result_end += 1
        logger.debug("slice result %s:%s end %s", start, result_end, end)
        result = test_file.get_tokens(start, result_end)
        return result, result_end

    @staticmethod
    def convert_request(sql):
        if sql.startswith("CREATE TABLE"):
            result = sqlglot.transpile(sql, read="sqlite", write="clickhouse")[0]
            pk_token = sqlglot.parse_one(result, read="clickhouse").find(
                PrimaryKeyColumnConstraint
            )
            pk_string = "tuple()"
            if pk_token is not None:
                pk_string = str(pk_token.find_ancestor(ColumnDef).args["this"])

            result += " ENGINE = MergeTree() ORDER BY " + pk_string
            return result
        elif "SELECT" in sql and "CAST" in sql and "NULL" in sql:
            # convert `CAST (NULL as INTEGER)` to `CAST (NULL as Nullable(Int32))`
            try:
                ast = sqlglot.parse_one(sql, read="sqlite")
            except sqlglot.errors.ParseError as err:
                logger.info("cannot parse %s , error is %s", sql, err)
                return sql
            cast = ast.find(sqlglot.expressions.Cast)
            # logger.info("found sql %s && %s && %s", sql, cast.sql(), cast.to.args)
            if (
                cast is not None
                and cast.name == "NULL"
                and ("nested" not in cast.to.args or not cast.to.args["nested"])
            ):
                cast.args["to"] = sqlglot.expressions.DataType.build(
                    "NULLABLE", expressions=[cast.to]
                )
                new_sql = ast.sql("clickhouse")
                # logger.info("convert from %s to %s", sql, new_sql)
                return new_sql
        return sql

    @staticmethod
    def parse_block(parser, start, end):
        file_pos = FileAndPos(parser.get_test_name(), start + 1)
        logger.debug("%s start %s end %s", file_pos, start, end)

        block_type = BlockType.comments
        conditions = []
        controls = []
        statement = None
        query = None
        request = []
        result_line = None
        result = []

        line = start
        while line < end:
            tokens = parser.get_tokens(line)

            if _is_comment_line(tokens):
                pass
            elif _is_conditional_line(tokens):
                conditions.append(parser.get_tokens(line))

            elif _is_control_line(tokens):
                assert block_type in (BlockType.comments, BlockType.control)
                block_type = BlockType.control
                controls.append(parser.get_tokens(line))

            elif _is_statement_line(tokens):
                assert block_type in (BlockType.comments,)
                block_type = BlockType.statement
                statement = parser.get_tokens(line)
                request, last_line = FileBlockBase.__parse_request(
                    parser, line + 1, end
                )
                if parser.dbms_name == "ClickHouse":
                    request = FileBlockBase.convert_request(request)
                assert last_line == end
                line = last_line

            elif _is_query_line(tokens):
                assert block_type in (BlockType.comments,)
                block_type = BlockType.query
                query = parser.get_tokens(line)
                request, last_line = FileBlockBase.__parse_request(
                    parser, line + 1, end
                )
                if parser.dbms_name == "ClickHouse":
                    request = FileBlockBase.convert_request(request)
                result_line = last_line
                line = last_line
                if line == end:
                    break
                tokens = parser.get_tokens(line)
                assert _is_separation_line(tokens), f"last_line {last_line}, end {end}"
                result, last_line = FileBlockBase.__parse_result(parser, line + 1, end)
                assert last_line == end
                line = last_line
            line += 1

        if block_type == BlockType.comments:
            return FileBlockComments(parser, start, end)

        if block_type == BlockType.control:
            return FileBlockControl(parser, start, end, conditions, controls)

        if block_type == BlockType.statement:
            return FileBlockStatement(
                parser, start, end, conditions, statement, request
            )

        if block_type == BlockType.query:
            block = FileBlockQuery(
                parser, start, end, conditions, query, request, result_line
            )
            block.with_result(result)
            return block
        raise ValueError(f"Unknown block_type {block_type}")

    def dump_to(self, output):
        if output is None:
            return
        for line in range(self._start, self._end):
            output.write(self._parser.get_line(line))
        output.write("\n")


class FileBlockComments(FileBlockBase):
    def get_block_type(self):
        return BlockType.comments


class FileBlockControl(FileBlockBase):
    def __init__(self, parser, start, end, conditions, control):
        super().__init__(parser, start, end)
        self.conditions = conditions
        self.control = control

    def get_block_type(self):
        return BlockType.control

    def get_conditions(self):
        return self.conditions


class FileBlockStatement(FileBlockBase):
    def __init__(self, parser, start, end, conditions, statement, request):
        super().__init__(parser, start, end)
        self.conditions = conditions
        self.statement = statement
        self.request = request

    def get_block_type(self):
        return BlockType.statement

    def get_request(self):
        return self.request

    def get_conditions(self):
        return self.conditions

    def get_statement(self):
        return self.statement

    def expected_error(self):
        return self.statement[1] == "error"


class FileBlockQuery(FileBlockBase):
    def __init__(self, parser, start, end, conditions, query, request, result_line):
        super().__init__(parser, start, end)
        self.conditions = conditions
        self.query = query
        self.request = request
        self.result = None
        self.result_line = result_line

    def get_block_type(self):
        return BlockType.query

    def get_request(self):
        return self.request

    def get_conditions(self):
        return self.conditions

    def get_query(self):
        return self.query

    def expected_error(self):
        return " ".join(self.query[2:]).lower() if self.query[1] == "error" else None

    def get_types(self):
        if self.query[1] == "error":
            raise TestFileFormatException(
                "the query is expected to fail, there are no types"
            )
        return self.query[1]

    def get_sort_mode(self):
        return self.query[2]

    def get_result(self):
        return self.result

    def with_result(self, result):
        self.result = result

    def dump_to(self, output):
        if output is None:
            return

        for line in range(self._start, self.result_line):
            output.write(self._parser.get_line(line))

        if self.result is not None:
            logger.debug("dump result %s", self.result)
            output.write("----\n")
            for row in self.result:
                output.write(" ".join(row) + "\n")

        output.write("\n")


class TestFileParser:
    CONTROL_TOKENS = ["halt", "hash-threshold"]
    CONDITIONS_TOKENS = [CONDITION_SKIP, CONDITION_ONLY]
    STATEMENT_TOKEN = "statement"
    QUERY_TOKEN = "query"
    COMMENT_TOKEN = "#"

    DEFAULT_HASH_THRESHOLD = 8

    def __init__(self, stream, test_name, test_file, dbms_name):
        self._stream = stream
        self._test_name = test_name
        self._test_file = test_file
        self.dbms_name = dbms_name

        self._lines = []
        self._raw_tokens = []
        self._tokens = []
        self._empty_lines = []

    def get_test_name(self):
        return self._test_name

    def get_test_file(self):
        if self._test_file is not None:
            return self._test_file
        return self._test_name

    def get_line(self, line):
        return self._lines[line]

    def get_tokens(self, start, end=None):
        if end is None:
            return self._tokens[start]
        else:
            return self._tokens[start:end]

    def get_tokens_from_lines(self, start, end):
        return list(chain(*self._tokens[start:end]))

    def __load_file(self):
        self._lines = self._stream.readlines()

        self._raw_tokens = [line.split() for line in self._lines]
        assert len(self._lines) == len(self._raw_tokens)

        self._tokens = []
        for line in self._raw_tokens:
            if self.COMMENT_TOKEN in line:
                comment_starts_at = line.index(self.COMMENT_TOKEN)
                self._tokens.append(line[0:comment_starts_at])
            else:
                self._tokens.append(line)

        self._empty_lines = [i for i, x in enumerate(self._raw_tokens) if len(x) == 0]

        logger.debug(
            "Test file %s loaded rows %s, empty rows %s",
            self.get_test_file(),
            len(self._lines),
            len(self._empty_lines),
        )

    def __unload_file(self):
        self._test_file = None
        self._test_name = None
        self._stream = None
        self._lines = []
        self._raw_tokens = []
        self._tokens = []
        self._empty_lines = []

    def _iterate_blocks(self):
        prev = 0
        for i in self._empty_lines:
            if prev != i:
                yield FileBlockBase.parse_block(self, prev, i)
            prev = i + 1

        if prev != len(self._lines):
            yield FileBlockBase.parse_block(self, prev, len(self._lines))

    def test_blocks(self):
        try:
            self.__load_file()
            yield from self._iterate_blocks()
        finally:
            self.__unload_file()


class QueryResult:
    def __init__(
        self,
        rows=None,
        values_count=None,
        data_hash=None,
        exception=None,
        hash_threshold=0,
    ):
        self.rows = rows
        self.values_count = values_count
        self.data_hash = data_hash
        self.exception = exception
        self.hash_threshold = hash_threshold
        self.hash_it()
        logger.debug("created QueryResult %s", str(self))

    def __str__(self):
        params = ", ".join(
            (
                str(x)
                for x in [
                    f"rows: {self.rows}" if self.rows else "",
                    f"values_count: {self.values_count}" if self.values_count else "",
                    f"data_hash: {self.data_hash}" if self.data_hash else "",
                    f"exception: {self.exception}" if self.exception else "",
                    (
                        f"hash_threshold: {self.hash_threshold}"
                        if self.hash_threshold
                        else ""
                    ),
                ]
                if x
            )
        )
        return f"QueryResult({params})"

    def __iter__(self):
        if self.rows is not None:
            if self.hash_threshold == 0:
                return iter(self.rows)
            if self.values_count <= self.hash_threshold:
                return iter(self.rows)
        if self.data_hash is not None:
            return iter([[f"{self.values_count} values hashing to {self.data_hash}"]])
        if self.exception is not None:
            return iter([[f"exception: {self.exception}"]])
        raise ProgramError("Query result is empty", details=str(self))

    @staticmethod
    def __value_count(rows):
        return reduce(lambda a, b: a + len(b), rows, 0)

    @staticmethod
    def parse_it(rows, hash_threshold):
        logger.debug("parse result len: %s rows: %s", len(rows), rows)
        if len(rows) == 1:
            logger.debug("one row is %s", rows)
            if len(rows[0]) > 0 and rows[0][0] == "exception:":
                logging.debug("as exception")
                message = " ".join(rows[0][1:])
                return QueryResult(exception=message)
            if len(rows[0]) == 5 and " ".join(rows[0][1:4]) == "values hashing to":
                logging.debug("as hashed data")
                values_count = int(rows[0][0])
                data_hash = rows[0][4]
                return QueryResult(data_hash=data_hash, values_count=values_count)
        logger.debug("as data")
        values_count = QueryResult.__value_count(rows)
        return QueryResult(
            rows=rows, values_count=values_count, hash_threshold=hash_threshold
        )

    @staticmethod
    def __result_as_strings(rows, types):
        res = []
        for row in rows:
            res_row = []
            for c, t in zip(row, types):
                logger.debug("Building row. c:%s t:%s", c, t)
                if c is None:
                    res_row.append("NULL")
                    continue

                if t == "T":
                    if c == "":
                        res_row.append("(empty)")
                    else:
                        res_row.append(str(c))
                elif t == "I":
                    try:
                        res_row.append(str(int(c)))
                    except ValueError:
                        # raise QueryExecutionError(
                        #     f"Got non-integer result '{c}' for I type."
                        # )
                        res_row.append(str(int(0)))
                    except OverflowError as ex:
                        raise QueryExecutionError(
                            f"Got overflowed result '{c}' for I type."
                        ) from ex

                elif t == "R":
                    res_row.append(f"{c:.3f}")

            res.append(res_row)
        return res

    @staticmethod
    def __sort_result(rows, sort_mode):
        if sort_mode == "nosort":
            return rows
        if sort_mode == "rowsort":
            return sorted(rows)
        if sort_mode == "valuesort":
            values = list(chain(*rows))
            values.sort()
            return [values] if values else []
        return []

    @staticmethod
    def __calculate_hash(rows):
        md5_hash = md5()
        for row in rows:
            for value in row:
                md5_hash.update(value.encode("ascii"))
        return str(md5_hash.hexdigest())

    @staticmethod
    def make_it(rows, types, sort_mode, hash_threshold):
        values_count = QueryResult.__value_count(rows)
        as_string = QueryResult.__result_as_strings(rows, types)
        as_sorted = QueryResult.__sort_result(as_string, sort_mode)
        return QueryResult(
            rows=as_sorted, values_count=values_count, hash_threshold=hash_threshold
        )

    def hash_it(self):
        if self.rows is not None and self.data_hash is None:
            self.data_hash = QueryResult.__calculate_hash(self.rows)
        return self

    @staticmethod
    def as_exception(e):
        # do not print details to the test file
        # but print original exception
        if isinstance(e, ErrorWithParent):
            message = f"{e}, original is: {e.get_parent()}"
        else:
            message = str(e)

        return QueryResult(exception=message)

    @staticmethod
    def assert_eq(canonic, actual):
        if not isinstance(canonic, QueryResult):
            raise ProgramError("NotImplemented")

        if not isinstance(actual, QueryResult):
            raise ProgramError("NotImplemented")

        if canonic.exception is not None or actual.exception is not None:
            if canonic.exception is not None and actual.exception is not None:
                if canonic.exception != actual.exception:
                    raise DataResultDiffer(
                        "canonic and actual results have different exceptions",
                        details=f"canonic: {canonic.exception}, actual: {actual.exception}",
                    )
                # exceptions are the same
                return
            elif canonic.exception is not None:
                raise DataResultDiffer(
                    "canonic result has exception and actual result doesn't",
                    details=f"canonic: {canonic.exception}",
                )
            else:
                raise DataResultDiffer(
                    "actual result has exception and canonic result doesn't",
                    details=f"actual: {actual.exception}",
                )

        canonic.hash_it()
        actual.hash_it()

        if canonic.data_hash is not None:
            if actual.data_hash is None:
                raise ProgramError("actual result has to have hash for data")
            if canonic.values_count != actual.values_count:
                raise DataResultDiffer(
                    "canonic and actual results have different value count",
                    details=f"canonic values count {canonic.values_count}, "
                    f"actual {actual.values_count}",
                )
            if canonic.data_hash != actual.data_hash:
                raise DataResultDiffer(
                    "canonic and actual results have different hashes"
                )
            return

        if canonic.rows is not None and actual.rows is not None:
            if canonic.values_count != actual.values_count:
                raise DataResultDiffer(
                    "canonic and actual results have different value count",
                    details=f"canonic values count {canonic.values_count}, "
                    f"actual {actual.values_count}",
                )
            if canonic.rows != actual.rows:
                raise DataResultDiffer(
                    "canonic and actual results have different values"
                )
            return

        raise ProgramError(
            "Unable to compare results",
            details=f"actual {actual}, canonic {canonic}",
        )
