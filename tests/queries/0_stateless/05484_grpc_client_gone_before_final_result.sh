#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the grpc library is not built

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The client cancels the call while the query runs, so sending the final result fails.
# `SHOW TABLES` runs an internal query, whose callbacks use a context of their own.
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
    deadline = time.monotonic() + 30
    while not predicate():
        if time.monotonic() > deadline:
            raise TimeoutError
        time.sleep(0.05)


with grpc.insecure_channel(f"localhost:{os.environ.get('CLICKHOUSE_PORT_GRPC', '9100')}") as channel:
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)

    def query(text):
        result = stub.ExecuteQuery(clickhouse_grpc_pb2.QueryInfo(
            query=text, database=database, output_format="TabSeparated",
            settings={"max_rows_to_read": "0", "enable_parallel_replicas": "0"}), timeout=60)
        if result.HasField("exception"):
            raise RuntimeError(result.exception.display_text)
        return result.output.decode().strip()

    for case, text in (("show", "SHOW TABLES FROM system WHERE sleep(1) = 0"), ("select", "SELECT sleep(2)")):
        # Retry when the query finishes before it is seen running.
        for _ in range(10):
            query_id = f"{database}_{case}_{uuid.uuid4().hex}"
            future = stub.ExecuteQuery.future(
                clickhouse_grpc_pb2.QueryInfo(query=text, query_id=query_id, database=database), timeout=60)
            running = lambda: query(f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'") == "1"
            wait_until(lambda: future.done() or running())
            if future.cancel():
                break

        wait_until(lambda: query(f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'") == "0")

        def failed_to_send():
            query("SYSTEM FLUSH LOGS text_log")
            return query(f"""SELECT count() FROM system.text_log WHERE event_date >= yesterday() AND query_id = '{query_id}'
                AND message LIKE '%Failed to send result to the client%'""") != "0"
        wait_until(failed_to_send)

        query("SYSTEM FLUSH LOGS query_log")
        print(case, query(f"""SELECT arrayStringConcat(groupArray(toString(type)), ' ') FROM (
            SELECT type FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase()
            AND query_id = '{query_id}' ORDER BY event_time_microseconds)"""))
EOF
