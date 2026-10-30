#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the grpc library is not built

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# `Pretty` formats write the squashed blocks from a background thread, into the output buffer of the gRPC call,
# while the call reads and sends that buffer between the blocks.
python3 - "$CURDIR/../../../utils/grpc-client/pb2" <<'EOF'
import os
import re
import sys
sys.path.insert(0, sys.argv[1])
import grpc
import clickhouse_grpc_pb2
import clickhouse_grpc_pb2_grpc

settings = {"max_block_size": "1", "max_threads": "1", "output_format_pretty_squash_consecutive_ms": "1", "interactive_delay": "10000"}
query = "SELECT number FROM numbers(4) WHERE sleepEachRow(0.05) = 0{} FORMAT PrettyCompactNoEscapes"

def values(output):
    return " ".join(re.findall(r"│\s+(\d+)\s+│", output.decode()))

with grpc.insecure_channel(f"localhost:{os.environ.get('CLICKHOUSE_PORT_GRPC', '9100')}") as channel:
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
    query_info = clickhouse_grpc_pb2.QueryInfo(query=query.format(""), settings=settings)
    print(values(stub.ExecuteQuery(query_info, timeout=60).output))
    print(values(b"".join(result.output for result in stub.ExecuteQueryWithStreamOutput(query_info, timeout=60))))
    # The exception arrives while the writer may still hold a squashed chunk.
    query = "SELECT number FROM numbers(4) WHERE if(number < 2, sleepEachRow(0.05) = 0, throwIf(number = 2) = 0) FORMAT PrettyCompactNoEscapes"
    codes = set()
    for squash_ms in range(1, 6):
        for _ in range(4):
            query_info = clickhouse_grpc_pb2.QueryInfo(
                query=query, settings={**settings, "output_format_pretty_squash_consecutive_ms": str(squash_ms)})
            codes.add(stub.ExecuteQuery(query_info, timeout=60).exception.code)
    print(*sorted(codes))
    print(stub.ExecuteQuery(clickhouse_grpc_pb2.QueryInfo(query="SELECT 1"), timeout=60).output.decode(), end="")
EOF
