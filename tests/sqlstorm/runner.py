#!/usr/bin/env python3

"""
SQLStorm benchmark runner for ClickHouse.

Downloads the StackOverflow dataset, creates tables, loads data, and runs
LLM-generated analytical queries from the SQLStorm benchmark suite.

Usage:
    python3 runner.py --query-dir <path> --data-dir <path> --out-dir <path>
        [--schema <path>] [--timeout <seconds>] [--port <port>]
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import time

# Import the query rewriter for PostgreSQL -> ClickHouse dialect conversion
sys.path.insert(0, os.path.dirname(__file__))
from rewrite_queries import rewrite_query


# ClickHouse type mapping from the OLAPBench schema types
TYPE_MAP = {
    "smallint": "Int16",
    "int": "Int32",
    "bigint": "Int64",
    "bool": "Bool",
    "text": "String",
    "timestamp": "DateTime64(6)",
}


# ClickHouse settings matching OLAPBench's clickhouse adapter.
# data_type_default_nullable is used only for schema creation and data loading,
# not for query execution — it causes Nullable(Array(...)) errors with groupArray.
CLICKHOUSE_SETTINGS = [
    "allow_experimental_join_condition = 1",
    "allow_experimental_analyzer = 1",
    "use_query_cache = 0",
    "union_default_mode = 'DISTINCT'",
    "join_use_nulls = 1",
    "group_by_use_nulls = 1",
    "prefer_column_name_to_alias = 1",
    "cast_keep_nullable = 1",
    # aggregate_functions_null_for_empty is NOT set because it makes groupArray
    # return Nullable(Array(...)) which is illegal in ClickHouse's type system.
]

# Additional settings for schema creation and data loading only
SCHEMA_SETTINGS = CLICKHOUSE_SETTINGS + [
    "data_type_default_nullable = 1",
]


def map_type(sql_type):
    """Convert a schema type string to a ClickHouse type."""
    lower = sql_type.lower().strip()
    not_null = "not null" in lower
    base = lower.replace("not null", "").strip()

    # Handle varchar(N) -> String
    if base.startswith("varchar"):
        ch_type = "String"
    else:
        ch_type = TYPE_MAP.get(base, "String")

    if not_null:
        return ch_type
    return f"Nullable({ch_type})"


def generate_create_table(table, database):
    """Generate a CREATE TABLE statement from a schema table definition."""
    columns = []
    for col in table["columns"]:
        ch_type = map_type(col["type"])
        columns.append(f'    "{col["name"]}" {ch_type}')

    pk_col = table.get("primary key", {}).get("column", "")
    order_by = f'"{pk_col}"' if pk_col else "tuple()"

    cols_str = ",\n".join(columns)
    return (
        f'CREATE TABLE IF NOT EXISTS {database}."{table["name"]}"\n'
        f"(\n{cols_str}\n)\n"
        f"ENGINE = MergeTree\n"
        f"ORDER BY ({order_by})\n"
        f"SETTINGS allow_nullable_key = 1"
    )


def run_clickhouse_query(query, port=9000, database="sqlstorm", timeout=60, settings=None):
    """Execute a query via clickhouse-client. Returns (success, stdout, stderr, elapsed_ms)."""
    if settings is None:
        settings = CLICKHOUSE_SETTINGS
    cmd = [
        "clickhouse-client",
        "--port", str(port),
        "--database", database,
        "--time",
        "--format", "Null",
        "--max_execution_time", str(timeout),
    ]
    for setting in settings:
        key, _, val = setting.partition("=")
        cmd.extend([f"--{key.strip()}", val.strip().strip("'")])
    cmd.extend(["--query", query])

    start = time.monotonic()
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout + 30,  # OS-level timeout with margin
        )
        elapsed_ms = (time.monotonic() - start) * 1000
        return result.returncode == 0, result.stdout, result.stderr, elapsed_ms
    except subprocess.TimeoutExpired:
        elapsed_ms = (time.monotonic() - start) * 1000
        return False, "", "OS-level timeout expired", elapsed_ms


def create_schema(schema, port, database):
    """Create the database and all tables from the schema definition."""
    print(f"Creating database {database}")
    subprocess.run(
        ["clickhouse-client", "--port", str(port),
         "--query", f"CREATE DATABASE IF NOT EXISTS {database}"],
        check=True,
    )

    for table in schema["tables"]:
        ddl = generate_create_table(table, database)
        print(f"  Creating table {table['name']}")
        ok, _, err, _ = run_clickhouse_query(ddl, port=port, database=database, settings=SCHEMA_SETTINGS)
        if not ok:
            print(f"  ERROR creating {table['name']}: {err}", file=sys.stderr)
            return False
    return True


def load_data(schema, data_dir, port, database):
    """Load CSV data files into ClickHouse tables."""
    if not os.path.isdir(data_dir):
        print(f"Data directory not found: {data_dir}")
        return False

    delimiter = schema.get("delimiter", ",")

    for table in schema["tables"]:
        table_name = table["name"]
        # The OLAPBench data files are named <TableName>.csv
        csv_file = os.path.join(data_dir, f"{table_name}.csv")
        if not os.path.isfile(csv_file):
            print(f"  No data file for {table_name}, skipping")
            continue

        file_size_mb = os.path.getsize(csv_file) / (1024 * 1024)
        print(f"  Loading {table_name} ({file_size_mb:.1f} MB)")

        settings_args = []
        for s in SCHEMA_SETTINGS:
            key, _, val = s.partition("=")
            settings_args.extend([f"--{key.strip()}", val.strip().strip("'")])
        if delimiter != ",":
            settings_args.extend(["--format_csv_delimiter", delimiter])

        # Use INSERT FROM INFILE for fast loading; CSVWithNames skips the header row
        query = f'INSERT INTO {database}."{table_name}" FROM INFILE \'{csv_file}\' FORMAT CSVWithNames'
        cmd = [
            "clickhouse-client",
            "--port", str(port),
            "--database", database,
            "--query", query,
            *settings_args,
        ]

        start = time.monotonic()
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        elapsed = time.monotonic() - start

        if result.returncode != 0:
            print(f"  ERROR loading {table_name}: {result.stderr}", file=sys.stderr)
            return False
        print(f"  Loaded {table_name} in {elapsed:.1f}s")

    return True


def classify_error(stderr):
    """Classify an error message into a category."""
    lower = stderr.lower()
    if "connection refused" in lower or "network_error" in lower:
        return "connection_error"
    if "timeout" in lower or "time limit" in lower or "time_limit" in lower or "max_execution_time" in lower or "timeout_exceeded" in lower:
        return "timeout"
    if "memory limit" in lower or "memory_limit" in lower:
        return "oom"
    if "syntax error" in lower or "parse error" in lower:
        return "syntax_error"
    if ("unknown function" in lower or "function with name" in lower) and "does not exist" in lower:
        return "unknown_function"
    if "unknown identifier" in lower or "cannot be resolved" in lower or "unknown expression" in lower:
        return "unknown_identifier"
    if "not an aggregate" in lower or "not in group by" in lower:
        return "not_an_aggregate"
    if "not implemented" in lower or "not_implemented" in lower:
        return "not_implemented"
    if "unknown database" in lower or "database" in lower and "does not exist" in lower:
        return "unknown_database"
    if "invalid_join_on" in lower or "cannot determine join" in lower:
        return "invalid_join"
    return "error"


def wait_for_server(port, max_attempts=30, delay=2):
    """Wait for ClickHouse server to become ready."""
    for attempt in range(max_attempts):
        try:
            result = subprocess.run(
                ["clickhouse-client", "--port", str(port), "--query", "SELECT 1"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                return True
        except (subprocess.TimeoutExpired, OSError):
            pass
        time.sleep(delay)
    return False


def ensure_database(port, database, schema, data_dir):
    """Ensure the database exists; recreate schema and reload data if needed."""
    # Check if database exists using a direct client call (run_clickhouse_query uses --format Null)
    try:
        result = subprocess.run(
            ["clickhouse-client", "--port", str(port),
             "--query", f"SELECT count() FROM system.databases WHERE name = '{database}'"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip() == "1":
            return True
    except (subprocess.TimeoutExpired, OSError):
        pass

    print(f"  Database {database} lost, recreating schema and reloading data...")
    if not create_schema(schema, port, database):
        return False
    if not load_data(schema, data_dir, port, database):
        return False
    print(f"  Database {database} restored")
    return True


def run_queries(query_dir, port, database, timeout, out_dir, schema=None, data_dir=None):
    """Run all SQL query files and collect results."""
    results = []

    query_files = sorted(
        [f for f in os.listdir(query_dir) if f.endswith(".sql")],
        key=lambda f: int(os.path.splitext(f)[0]) if os.path.splitext(f)[0].isdigit() else f,
    )

    total = len(query_files)
    print(f"Running {total} queries (timeout={timeout}s)")

    consecutive_conn_errors = 0
    max_conn_errors = 5  # trigger recovery after this many consecutive connection errors

    for i, qf in enumerate(query_files, 1):
        query_path = os.path.join(query_dir, qf)
        query_name = os.path.splitext(qf)[0]

        # Check for ClickHouse-specific override
        ch_override = query_path + ".clickhouse"
        if os.path.isfile(ch_override):
            query_path = ch_override

        with open(query_path, "r") as f:
            query = f.read().strip()

        if not query:
            continue

        # Apply PostgreSQL -> ClickHouse dialect rewrites
        query = rewrite_query(query)

        ok, stdout, stderr, elapsed_ms = run_clickhouse_query(
            query, port=port, database=database, timeout=timeout
        )

        if ok:
            state = "success"
            message = ""
            consecutive_conn_errors = 0
        else:
            state = classify_error(stderr)
            message = stderr.strip()[:500]

            # Detect server crash / connection errors and attempt recovery
            if state in ("connection_error", "unknown_database"):
                consecutive_conn_errors += 1
                if consecutive_conn_errors >= max_conn_errors:
                    print(f"  Server appears down after {consecutive_conn_errors} consecutive errors, attempting recovery...")
                    if wait_for_server(port):
                        if schema and data_dir and ensure_database(port, database, schema, data_dir):
                            consecutive_conn_errors = 0
                            # Retry the current query
                            ok, stdout, stderr, elapsed_ms = run_clickhouse_query(
                                query, port=port, database=database, timeout=timeout
                            )
                            if ok:
                                state = "success"
                                message = ""
                            else:
                                state = classify_error(stderr)
                                message = stderr.strip()[:500]
                        else:
                            print("  Failed to restore database, continuing...")
                    else:
                        print("  Server did not recover, continuing...")
            else:
                consecutive_conn_errors = 0

        results.append({
            "query": query_name,
            "state": state,
            "client_total_ms": round(elapsed_ms, 3),
            "message": message,
        })

        if i % 100 == 0 or i == total:
            success_count = sum(1 for r in results if r["state"] == "success")
            print(f"  Progress: {i}/{total} ({success_count} success)")

    # Write results JSON
    results_path = os.path.join(out_dir, "results.json")
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)

    # Write results CSV
    csv_path = os.path.join(out_dir, "results.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["query", "state", "client_total_ms", "message"])
        writer.writeheader()
        writer.writerows(results)

    return results


def compute_stats(results):
    """Compute summary statistics from query results."""
    total = len(results)
    success = sum(1 for r in results if r["state"] == "success")
    timeout = sum(1 for r in results if r["state"] == "timeout")
    oom = sum(1 for r in results if r["state"] == "oom")
    errors = total - success - timeout - oom

    success_times = [r["client_total_ms"] for r in results if r["state"] == "success"]
    median_ms = sorted(success_times)[len(success_times) // 2] if success_times else 0
    sum_ms = sum(success_times)

    # Classify errors by category
    error_categories = {}
    for r in results:
        if r["state"] != "success":
            cat = r["state"]
            error_categories[cat] = error_categories.get(cat, 0) + 1

    return {
        "total": total,
        "success": success,
        "timeout": timeout,
        "oom": oom,
        "errors": errors,
        "success_rate": success / total if total else 0,
        "median_ms": round(median_ms, 3),
        "sum_ms": round(sum_ms, 3),
        "error_categories": error_categories,
    }


def main():
    parser = argparse.ArgumentParser(description="SQLStorm benchmark runner for ClickHouse")
    parser.add_argument("--query-dir", required=True, help="Directory containing .sql query files")
    parser.add_argument("--data-dir", required=True, help="Directory containing CSV data files")
    parser.add_argument("--schema", required=True, help="Path to dbschema.json")
    parser.add_argument("--out-dir", required=True, help="Output directory for results")
    parser.add_argument("--timeout", type=int, default=10, help="Per-query timeout in seconds")
    parser.add_argument("--port", type=int, default=9000, help="ClickHouse native port")
    parser.add_argument("--database", default="sqlstorm", help="Database name")
    parser.add_argument("--skip-schema", action="store_true", help="Skip schema creation")
    parser.add_argument("--skip-load", action="store_true", help="Skip data loading")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)

    # Load schema
    with open(args.schema, "r") as f:
        schema = json.load(f)

    # Create schema
    if not args.skip_schema:
        if not create_schema(schema, args.port, args.database):
            print("Schema creation failed", file=sys.stderr)
            sys.exit(1)

    # Load data
    if not args.skip_load:
        if not load_data(schema, args.data_dir, args.port, args.database):
            print("Data loading failed", file=sys.stderr)
            sys.exit(1)

    # Run queries
    results = run_queries(
        args.query_dir, args.port, args.database, args.timeout, args.out_dir,
        schema=schema, data_dir=args.data_dir,
    )

    # Compute and save stats
    stats = compute_stats(results)
    stats_path = os.path.join(args.out_dir, "stats.json")
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)

    print(f"\nResults: {stats['success']}/{stats['total']} success "
          f"({stats['success_rate']:.1%}), "
          f"{stats['timeout']} timeout, {stats['oom']} OOM, {stats['errors']} errors")
    print(f"Median query time: {stats['median_ms']:.1f}ms, Total: {stats['sum_ms']:.0f}ms")

    sys.exit(0)


if __name__ == "__main__":
    main()
