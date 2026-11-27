#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the grpc library is not built
# Tag no-parallel: the failpoint fails the next gRPC call of any test

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# A call whose thread cannot start must not stop the server from completing the other calls.
trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT grpc_call_start_thread_fail"' EXIT
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT grpc_call_start_thread_fail"

python3 - "$CURDIR/../../../utils/grpc-client/pb2" <<'EOF_PYTHON'
import os
import sys
sys.path.insert(0, sys.argv[1])
import grpc
import clickhouse_grpc_pb2
import clickhouse_grpc_pb2_grpc

with grpc.insecure_channel(f"localhost:{os.environ.get('CLICKHOUSE_PORT_GRPC', '9100')}") as channel:
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)
    for i in range(2):
        try:
            result = stub.ExecuteQuery(clickhouse_grpc_pb2.QueryInfo(query=f"SELECT {i}", output_format="TabSeparated"), timeout=60)
            print(result.exception.display_text if result.HasField("exception") else result.output.decode().strip())
        except grpc.RpcError as e:
            print(e.code())
EOF_PYTHON
