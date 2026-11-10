#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the grpc library is not built

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A gRPC cancel stops work that checks only the process list: window function recalculation and `sleepEachRow`.
python3 - "$CURDIR/../../../utils/grpc-client/pb2" "$CLICKHOUSE_DATABASE" <<'EOF'
import os
import sys
import time
import uuid
sys.path.insert(0, sys.argv[1])
import grpc
import clickhouse_grpc_pb2
import clickhouse_grpc_pb2_grpc

database = sys.argv[2]


def wait_until(predicate):
    deadline = time.monotonic() + 60
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError
        time.sleep(0.05)


with grpc.insecure_channel(f"localhost:{os.environ.get('CLICKHOUSE_PORT_GRPC', '9100')}") as channel:
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)

    def query(text):
        result = stub.ExecuteQuery(clickhouse_grpc_pb2.QueryInfo(
            query=text, database=database, output_format="TabSeparated",
            settings={"max_rows_to_read": "0", "enable_parallel_replicas": "0"}))
        if result.HasField("exception"):
            raise RuntimeError(result.exception.display_text)
        return result.output.decode().strip()

    query("CREATE TABLE t (x UInt8) ENGINE = Memory")
    for case, text, condition, settings in (
        ("select", "SELECT sum(number) OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM numbers(2000000)",
            "read_rows >= 2000000", {"max_block_size": "2000000"}),
        ("insert select", "INSERT INTO t SELECT sleepEachRow(3) FROM numbers(100)",
            "1", {"max_block_size": "100", "function_sleep_max_microseconds_per_block": "600000000"}),
    ):
        query_id = f"{database}_{uuid.uuid4().hex}"

        def requests():
            yield clickhouse_grpc_pb2.QueryInfo(query=text, query_id=query_id, database=database,
                settings={"max_execution_time": "60", "max_threads": "1"} | settings)
            wait_until(lambda: query(f"SELECT count() FROM system.processes WHERE query_id = '{query_id}' AND {condition} AND elapsed > 1") == "1")
            yield clickhouse_grpc_pb2.QueryInfo(cancel=True)

        results = list(stub.ExecuteQueryWithStreamIO(requests(), timeout=300))
        exceptions = [result.exception.code for result in results if result.HasField("exception")]
        print(f"{case}:", f"exception {exceptions[0]}" if exceptions else "cancelled" if results[-1].cancelled else "finished")
EOF
