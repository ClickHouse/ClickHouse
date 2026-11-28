#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the grpc library is not built

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A gRPC query cancelled before it returns its first block must not store its result in the query cache.
python3 - "$CURDIR/../../../utils/grpc-client/pb2" "$CLICKHOUSE_DATABASE" <<'EOF'
import os
import sys
import uuid
sys.path.insert(0, sys.argv[1])
import grpc
import clickhouse_grpc_pb2
import clickhouse_grpc_pb2_grpc

database = sys.argv[2]
tag = f"{database}_{uuid.uuid4().hex}"
# The scalar subquery is evaluated during analysis, so the cancel arrives before the first block.
text = "SELECT (SELECT sum(sleepEachRow(0.1)) FROM numbers(30)), number FROM numbers(10)"
settings = {"use_query_cache": "1", "query_cache_tag": tag, "query_cache_nondeterministic_function_handling": "save",
            "function_sleep_max_microseconds_per_block": "600000000"}

with grpc.insecure_channel(f"localhost:{os.environ.get('CLICKHOUSE_PORT_GRPC', '9100')}") as channel:
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)

    def query(text, settings=None):
        result = stub.ExecuteQuery(clickhouse_grpc_pb2.QueryInfo(
            query=text, database=database, output_format="TabSeparated", settings=settings or {}))
        if result.HasField("exception"):
            raise RuntimeError(result.exception.display_text)
        return result.output.decode()

    def cache_entries():
        return query(f"SELECT count() FROM system.query_cache WHERE tag = '{tag}'").strip()

    def requests():
        yield clickhouse_grpc_pb2.QueryInfo(query=text, database=database, output_format="TabSeparated", settings=settings)
        yield clickhouse_grpc_pb2.QueryInfo(cancel=True)

    results = list(stub.ExecuteQueryWithStreamIO(requests(), timeout=300))
    exceptions = [result.exception.code for result in results if result.HasField("exception")]
    print(f"exception: {exceptions[0]}" if exceptions else f"cancelled: {results[-1].cancelled}", f"cache entries: {cache_entries()}")

    rows = query(text, settings).count("\n")
    print(f"rows: {rows}", f"cache entries: {cache_entries()}")
EOF
