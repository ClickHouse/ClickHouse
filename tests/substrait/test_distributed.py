#!/usr/bin/env python3
"""ClickHouse Substrait Exporter -> DataFusion Consumer via Arrow IPC."""

import subprocess
import sys
import os
import tempfile
import glob
import json
import re
import pyarrow as pa
import pyarrow.ipc as ipc
from substrait.gen.proto import plan_pb2
from google.protobuf import json_format

CLICKHOUSE_BIN = os.path.join(os.path.dirname(__file__), "../../build/programs/clickhouse")
TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "test_data")
DATAFUSION_WORKER = os.path.join(os.path.dirname(__file__), "datafusion_worker.py")
QUERIES_DIR = os.path.join(os.path.dirname(__file__), "queries")


def discover_tables() -> dict[str, str]:
    """Discover all CSV files in test_data and map table names to paths."""
    tables = {}
    for csv_file in glob.glob(os.path.join(TEST_DATA_DIR, "*.csv")):
        # Table name is filename without -100.csv suffix
        basename = os.path.basename(csv_file)
        table_name = re.sub(r'-\d+\.csv$', '', basename)
        tables[table_name] = csv_file
    return tables


def build_table_defs(tables: dict[str, str]) -> str:
    """Build CREATE TABLE statements for all tables."""
    defs = []
    for table_name, csv_path in tables.items():
        defs.append(f"CREATE TABLE {table_name} AS SELECT * FROM file('{csv_path}', 'CSVWithNames');")
    return "\n".join(defs) + "\n"


def get_substrait_json(sql: str, table_defs: str) -> str:
    query = table_defs + f"EXPLAIN SUBSTRAIT {sql}"
    cmd = [CLICKHOUSE_BIN, "local", "--query", query, "--allow_experimental_substrait=1", "--format", "RawBLOB"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ClickHouse error: {result.stderr}")
    return result.stdout


def json_to_substrait_bytes(json_str: str) -> bytes:
    plan = plan_pb2.Plan()
    json_format.Parse(json_str, plan)
    return plan.SerializeToString()


def run_datafusion_worker(substrait_path: str, tables: dict[str, str], output_path: str) -> None:
    tables_json = json.dumps(tables)
    result = subprocess.run(
        [sys.executable, DATAFUSION_WORKER, substrait_path, tables_json, output_path],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"DataFusion worker error: {result.stderr}")


def read_arrow_ipc(path: str) -> pa.Table:
    with pa.OSFile(path, "rb") as source:
        reader = ipc.open_file(source)
        return reader.read_all()


def run_query_test(query: str, query_name: str, table_defs: str, tables: dict[str, str]) -> tuple[bool, str]:
    with tempfile.TemporaryDirectory() as tmpdir:
        substrait_path = os.path.join(tmpdir, "plan.substrait")
        result_path = os.path.join(tmpdir, "result.arrow")

        substrait_json = get_substrait_json(query, table_defs)
        substrait_bytes = json_to_substrait_bytes(substrait_json)

        with open(substrait_path, "wb") as f:
            f.write(substrait_bytes)

        run_datafusion_worker(substrait_path, tables, result_path)
        result_table = read_arrow_ipc(result_path)
        
        return True, f"{result_table.num_rows} rows"


def main():
    if not os.path.exists(CLICKHOUSE_BIN):
        print(f"Error: ClickHouse binary not found at {CLICKHOUSE_BIN}")
        sys.exit(1)

    # Discover all available tables from test_data
    tables = discover_tables()
    if not tables:
        print("Error: No CSV files found in test_data/")
        sys.exit(1)
    
    print(f"Available tables: {', '.join(sorted(tables.keys()))}")
    
    # Build CREATE TABLE statements for all tables
    table_defs = build_table_defs(tables)

    query_files = sorted(glob.glob(os.path.join(QUERIES_DIR, "*.sql")))
    
    if not query_files:
        print("No query files found in queries/")
        sys.exit(1)

    print(f"Running {len(query_files)} query tests\n")
    print("=" * 70)
    
    passed = 0
    failed = 0
    results = []

    for query_file in query_files:
        query_name = os.path.basename(query_file)
        with open(query_file, "r") as f:
            lines = [l for l in f.readlines() if not l.strip().startswith("--")]
            query = "".join(lines).strip()

        try:
            success, msg = run_query_test(query, query_name, table_defs, tables)
            if success:
                passed += 1
                results.append((query_name, "✓ PASS", msg))
                print(f"✓ {query_name}: {msg}")
            else:
                failed += 1
                results.append((query_name, "✗ FAIL", msg))
                print(f"✗ {query_name}: {msg}")
        except Exception as e:
            failed += 1
            error_msg = str(e).split('\n')[0][:60]
            results.append((query_name, "✗ FAIL", error_msg))
            print(f"✗ {query_name}: {error_msg}")

    print("=" * 70)
    print(f"\nSummary: {passed} passed, {failed} failed out of {len(query_files)} tests")
    
    if failed > 0:
        print("\nFailed tests:")
        for name, status, msg in results:
            if "FAIL" in status:
                print(f"  - {name}: {msg}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
