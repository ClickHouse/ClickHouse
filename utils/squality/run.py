#!/usr/bin/env python3
"""Run SQuaLity's parsed records on an isolated ClickHouse HTTP endpoint."""

import argparse
from collections import Counter
from contextlib import contextmanager
import hashlib
import http.client
import json
import math
from pathlib import Path
import random
import re
import sqlite3
import sys
import time
import urllib.parse
import uuid


class DatabaseError(Exception):
    pass


class ResultEncodingError(Exception):
    pass


class ClickHouse:
    def __init__(self, endpoint, settings, database=None):
        url = urllib.parse.urlparse(endpoint)
        if url.scheme != "http" or url.hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise ValueError("Use an isolated local HTTP server or an SSH tunnel")
        self.host = url.hostname
        self.port = url.port or 8123
        self.settings = settings
        self.database = database

    def execute(self, sql):
        params = {**self.settings, "default_format": "JSONCompact", "wait_end_of_query": 1}
        if self.database:
            params["database"] = self.database
        connection = http.client.HTTPConnection(self.host, self.port, timeout=15)
        try:
            connection.request("POST", "/?" + urllib.parse.urlencode(params), sql.encode())
            response = connection.getresponse()
            body_bytes = response.read()
        finally:
            connection.close()
        try:
            body = body_bytes.decode()
        except UnicodeDecodeError as error:
            raise ResultEncodingError(str(error)) from error
        if response.status != 200:
            raise DatabaseError(body)
        if not body.strip():
            return []
        result = json.loads(body)
        if "exception" in result:
            raise DatabaseError(result["exception"])
        return result["data"]

    @contextmanager
    def isolated_database(self):
        # Only drop a database created successfully by this invocation.
        name = "squality_" + uuid.uuid4().hex
        self.execute(f"CREATE DATABASE {name}")
        self.database = name
        try:
            yield
        finally:
            self.database = None
            self.execute(f"DROP DATABASE {name} SYNC")


def scalar(value, kind, suite):
    if value is None:
        return "NULL"
    if suite == "sqlite":
        if kind == "I":
            return str(int(value))
        if kind == "R":
            return f"{float(value):.3f}"
    if isinstance(value, bool):
        return str(value)
    return str(value).replace("\0", "\\0") if value != "" else "(empty)"


def compare(rows, record, suite, SortType, ResultFormat):
    width = len(record.data_type)
    if any(len(row) != width for row in rows):
        return False, repr(rows), "column_count"
    values = [[scalar(value, kind, suite) for value, kind in zip(row, record.data_type)] for row in rows]
    if record.sort == SortType.ROW_SORT:
        values.sort()
    flat = [value for row in values for value in row]
    if record.sort == SortType.VALUE_SORT:
        flat.sort()
    text = "\n".join(flat) + ("\n" if flat else "")
    expected = record.result
    hashed = re.fullmatch(r"(\d+) values hashing to ([a-f0-9]{32})", expected.strip())
    if hashed:
        actual = f"{len(flat)} values hashing to {hashlib.md5(text.encode()).hexdigest()}"
        return actual == expected.strip(), actual, "hash"
    if record.res_format == ResultFormat.REGEX:
        negative = "<!REGEX>:" in expected
        pattern = expected.split(":", 1)[1]
        actual = "\n".join("\t".join(row) for row in values)
        return bool(re.search(pattern, actual, re.DOTALL)) != negative, actual, "regex"
    expected_values = expected.splitlines() if expected else []
    if record.res_format == ResultFormat.ROW_WISE:
        expected_rows = [row.split("\t") for row in expected_values]
        if record.sort == SortType.ROW_SORT:
            expected_rows.sort()
        expected_values = [value for row in expected_rows for value in row]
    if record.sort == SortType.VALUE_SORT:
        expected_values.sort()
    if suite == "sqlite":
        return flat == expected_values, text.rstrip("\n"), "values"
    if len(flat) != len(expected_values):
        return False, text.rstrip("\n"), "value_count"
    for actual, expected_value in zip(flat, expected_values):
        if actual == expected_value:
            continue
        if expected_value in {"True", "False"} and actual in {"1", "0"}:
            if (expected_value == "True") == (actual == "1"):
                continue
        # DuckDB's SLT oracle allows numerically equivalent formatting.
        try:
            left, right = float(actual), float(expected_value)
        except ValueError:
            return False, text.rstrip("\n"), "values"
        if not (math.isnan(left) and math.isnan(right)) and not math.isclose(left, right, rel_tol=1e-6, abs_tol=1e-9):
            return False, text.rstrip("\n"), "values"
    return True, text.rstrip("\n"), "values"


def run_file(path, args, client, classes):
    SLTParser, DTParser, Statement, Query, Control, SortType, ResultFormat = classes
    parser = SLTParser() if args.suite == "sqlite" else DTParser()
    parser.get_file_name(str(path))
    parser.get_file_content()
    # These DuckDB runner guards describe the host or the DuckDB harness, not
    # test SQL. Their conditions hold for this isolated Linux process.
    harmless_requirements = {"noforcestorage", "no_force_storage", "notwindows",
                             "64bit", "skip_reload", "no_latest_storage",
                             "no_alternative_verify", "no_extension_autoloading",
                             "no_vector_verification"}
    if args.suite == "duckdb":
        parser.test_content = re.sub(
            r"(?m)^require ([^\s]+)[^\n]*\n?",
            lambda match: "" if match[1] in harmless_requirements else match[0],
            parser.test_content,
        )
    # Reject connection/restart/loop directives instead of silently losing records.
    directives = re.findall(r"(?m)^(?:require|loop|foreach|endloop|endfor|restart|reconnect|load|mode|set|sleep|query[^\n]*|statement[^\n]*)[^\n]*", parser.test_content)
    unsupported = [line for line in directives if line.split()[0] in {"require", "loop", "foreach", "endloop", "endfor", "restart", "reconnect", "load", "mode", "set", "sleep"}
                   or line.startswith("statement") and len(line.split()) > 2
                   or line.startswith("query") and len(line.split()) > 2 and line.split()[2] not in {"nosort", "rowsort", "valuesort"}]
    if unsupported:
        yield {"file": str(path), "status": "unsupported_harness", "directives": unsupported[:10]}
        return
    parser.parse_file()
    reference = sqlite3.connect(":memory:") if args.suite == "sqlite" else None
    setup = []
    labels = {}
    original_settings = client.settings.copy()
    try:
        with client.isolated_database():
            for record in parser.get_records():
                item = {"file": str(path), "record": record.id, "sql": record.sql}
                if isinstance(record, Control):
                    yield {**item, "status": "unsupported_control"}
                    break
                if "clickhouse" not in record.db:
                    yield {**item, "status": "skipped_condition"}
                    continue
                # Source suites may administer their own server. Never let a
                # transferred record administer this ClickHouse instance.
                if re.match(r"(?is)^\s*(?:CREATE|DROP|ATTACH|DETACH|ALTER)\s+DATABASE\b|^\s*USE\b", record.sql) or re.search(
                    r"(?i)\b(?:system|default)\s*\.", record.sql
                ) or re.match(
                    r"(?is)^\s*(?:SYSTEM|KILL\s+QUERY|GRANT|REVOKE|(?:CREATE|ALTER|DROP)\s+(?:USER|ROLE|QUOTA|FUNCTION|SETTINGS\s+PROFILE))\b",
                    record.sql,
                ) or re.search(r"(?i)\bON\s+CLUSTER\b", record.sql
                ):
                    yield {**item, "status": "unsupported_database_admin"}
                    break
                if args.suite == "duckdb" and re.match(r"(?is)^\s*EXPLAIN\b", record.sql):
                    yield {**item, "status": "unsupported_plan_oracle"}
                    continue
                if args.suite == "duckdb" and "{DATA_DIR}" in record.sql:
                    yield {**item, "status": "unsupported_fixture"}
                    break
                if args.max_records and record.id >= args.max_records:
                    yield {**item, "status": "record_limit"}
                    break
                # Indexes are performance hints, but UNIQUE indexes promise semantics.
                if re.match(r"\s*CREATE\s+UNIQUE\s+INDEX", record.sql, re.I):
                    yield {**item, "status": "unsupported_unique_index"}
                    break
                # DuckDB test-runner diagnostics do not affect SQL results.
                if args.suite == "duckdb" and re.match(
                    r"(?is)^\s*SET\s+(?:debug_[a-z_]+|disabled_optimizers|threads|memory_limit|profiling_renderer_settings)\s*=",
                    record.sql,
                ):
                    yield {**item, "status": "skipped_dialect_setting"}
                    continue
                ref_rows, ref_error = None, None
                if reference:
                    try:
                        ref_rows = reference.execute(record.sql).fetchall()
                    except sqlite3.Error as error:
                        ref_error = str(error)
                started = time.monotonic()
                try:
                    rows = client.execute(record.sql)
                except ResultEncodingError as error:
                    yield {**item, "status": "unsupported_result_encoding", "error": str(error)}
                    continue
                except DatabaseError as error:
                    message = str(error)
                    code_match = re.search(r"Code: (\d+)", message)
                    expected_error = isinstance(record, Statement) and not record.status
                    status = "expected_error" if expected_error else "sql_exception"
                    yield {**item, "status": status, "error": message, "code": int(code_match[1]) if code_match else None,
                           "elapsed": time.monotonic() - started, "setup": setup, "reference_error": ref_error}
                    if not expected_error and isinstance(record, Statement):
                        yield {"file": str(path), "status": "blocked_setup", "remaining": len(parser.records) - record.id - 1}
                        break
                    continue
                item["elapsed"] = time.monotonic() - started
                if isinstance(record, Statement):
                    if record.status:
                        setup.append(record.sql)
                        setting = re.fullmatch(r"\s*SET\s+([a-zA-Z_][a-zA-Z_0-9]*)\s*=\s*('?[^';]+'?)\s*;?\s*", record.sql, re.I)
                        if setting:
                            client.settings[setting[1]] = setting[2].strip("'")
                        yield {**item, "status": "statement_pass"}
                    else:
                        yield {**item, "status": "expected_error_succeeded", "setup": setup}
                        # Successful unexpected DML can change all subsequent results.
                        break
                    continue
                try:
                    matched, actual, method = compare(rows, record, args.suite, SortType, ResultFormat)
                except (ValueError, OverflowError) as error:
                    yield {**item, "status": "comparison_error", "error": str(error), "rows": rows}
                    continue
                if record.label and not record.result:
                    previous = labels.setdefault(record.label, actual)
                    matched = previous == actual
                    method = "label"
                item.update(expected=record.result, actual=actual, method=method)
                if not matched:
                    item.update(setup=setup, rows=rows)
                    if reference:
                        if ref_error:
                            item.update(reference_error=ref_error, status="reference_exception")
                        else:
                            try:
                                ref_match, ref_actual, _ = compare(ref_rows, record, args.suite, SortType, ResultFormat)
                            except (ValueError, OverflowError) as error:
                                item.update(status="reference_comparison_error", reference_error=str(error))
                            else:
                                item.update(reference_actual=ref_actual, status="result_mismatch" if ref_match else "oracle_mismatch")
                    else:
                        item["status"] = "result_mismatch"
                else:
                    item["status"] = "query_pass"
                yield item
    finally:
        client.settings = original_settings
        if reference:
            reference.close()


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--squality", required=True, type=Path)
    cli.add_argument("--suite", choices=["sqlite", "duckdb"], required=True)
    cli.add_argument("--tests", required=True, type=Path)
    cli.add_argument("--endpoint", default="http://127.0.0.1:18173")
    cli.add_argument("--profile", choices=["default", "sql_standard"], default="sql_standard")
    cli.add_argument("--settings", type=Path, default=Path(__file__).with_name("settings.json"))
    cli.add_argument("--set", action="append", default=[], metavar="NAME=VALUE")
    cli.add_argument("--output", required=True, type=Path)
    cli.add_argument("--glob", default="**/*.test")
    cli.add_argument("--max-files", type=int, default=0)
    cli.add_argument("--max-records", type=int, default=0)
    cli.add_argument("--seed", type=int, default=20261005)
    cli.add_argument("--shard-count", type=int, default=1)
    cli.add_argument("--shard-index", type=int, default=0)
    cli.add_argument("--resume", action="store_true")
    args = cli.parse_args()
    if args.shard_count < 1 or not 0 <= args.shard_index < args.shard_count:
        cli.error("Require 0 <= --shard-index < --shard-count")
    sys.path.insert(0, str(args.squality.resolve()))
    from src.testparser import SLTParser, DTParser
    from src.utils import Statement, Query, Control, SortType, ResultFormat, DBMS_Set
    DBMS_Set.add("clickhouse")
    settings = {"default_table_engine": "Memory", "max_execution_time": 5,
                "max_memory_usage": 1073741824, "max_threads": 2,
                "output_format_json_quote_64bit_integers": 0, "output_format_json_quote_denormals": 1,
                **json.loads(args.settings.read_text())[args.profile]}
    for override in args.set:
        key, value = override.split("=", 1)
        settings[key] = value
    client = ClickHouse(args.endpoint, settings)
    # Validate every setting up front. Unknown settings abort the run.
    version = client.execute("SELECT version(), revision()")
    files = sorted(args.tests.glob(args.glob)) if args.tests.is_dir() else [args.tests]
    if args.max_files:
        random.Random(args.seed).shuffle(files)
        files = sorted(files[:args.max_files])
    files = [path for index, path in enumerate(files) if index % args.shard_count == args.shard_index]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    run_identity = {"version": version, "suite": args.suite, "files": [str(path) for path in files],
                    "settings": settings, "max_records": args.max_records}
    fingerprint = hashlib.sha256(json.dumps(run_identity, sort_keys=True).encode()).hexdigest()
    checkpoint_path = args.output.with_suffix(".checkpoint.json")
    counts = Counter()
    first_file = 0
    if args.resume:
        checkpoint = json.loads(checkpoint_path.read_text())
        if checkpoint["fingerprint"] != fingerprint:
            raise ValueError("Checkpoint does not match the requested run")
        first_file = checkpoint["next_file"]
        counts.update(checkpoint["counts"])
        output_mode = "r+b"
    else:
        output_mode = "wb"
    started = time.monotonic()
    records_seen = sum(counts.values())
    with args.output.open(output_mode) as out:
        if args.resume:
            out.truncate(checkpoint["offset"])
            out.seek(checkpoint["offset"])
        for index in range(first_file, len(files)):
            path = files[index]
            try:
                for item in run_file(path, args, client, (SLTParser, DTParser, Statement, Query, Control, SortType, ResultFormat)):
                    counts[item["status"]] += 1
                    out.write((json.dumps(item, ensure_ascii=False) + "\n").encode())
                    records_seen += 1
                    if records_seen % 10000 == 0:
                        out.flush()
                        print(json.dumps({"files_complete": index, "records": records_seen,
                                          "counts": counts}), flush=True)
            except (IndexError, KeyError, ValueError) as error:
                # Parser failures are distinct from server failures.
                counts["harness_error"] += 1
                out.write((json.dumps({"file": str(path), "status": "harness_error", "error": str(error)}) + "\n").encode())
                records_seen += 1
            out.flush()
            checkpoint = {"fingerprint": fingerprint, "next_file": index + 1,
                          "offset": out.tell(), "counts": counts}
            checkpoint_tmp = checkpoint_path.with_suffix(".json.tmp")
            checkpoint_tmp.write_text(json.dumps(checkpoint))
            checkpoint_tmp.replace(checkpoint_path)
            print(json.dumps({"files_complete": index + 1, "files_total": len(files),
                              "records": records_seen, "counts": counts}), flush=True)
    summary = {"version": version, "profile": args.profile, "settings": settings, "suite": args.suite,
               "files": len(files), "seed": args.seed, "max_records": args.max_records,
               "shard_count": args.shard_count, "shard_index": args.shard_index,
               "counts": counts, "seconds": time.monotonic() - started}
    args.output.with_suffix(".summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
