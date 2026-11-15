#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the grpc library is not built

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# The result does not fit into `max_memory_usage`, so the query fails while its output is being written.
python3 - "$CURDIR/../../../utils/grpc-client/pb2" <<'EOF'
import os
import sys
sys.path.insert(0, sys.argv[1])
import grpc
import clickhouse_grpc_pb2
import clickhouse_grpc_pb2_grpc

with grpc.insecure_channel(f"localhost:{os.environ.get('CLICKHOUSE_PORT_GRPC', '9100')}") as channel:
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
    for query, settings, compression in (
        ("SELECT number FROM numbers(10000000)", {"max_memory_usage": "50000000"}, ""),
        ("SELECT randomString(1000) FROM numbers(50000)", {"max_memory_usage": "50000000", "max_block_size": "1000"}, "lz4"),
    ):
        result = stub.ExecuteQuery(clickhouse_grpc_pb2.QueryInfo(
            query=query, settings=settings, output_format="TabSeparated", output_compression_type=compression), timeout=60)
        print(result.exception.code)
    print(stub.ExecuteQuery(clickhouse_grpc_pb2.QueryInfo(query="SELECT 1"), timeout=60).output.decode(), end="")
EOF
