#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the grpc library is not built
# Tag no-replicated-database: the replicas listen on the same gRPC port, so the query can run on a server whose text_log is not read

# The input data of a gRPC INSERT with a field the table does not have: the field is skipped and the server logs a warning naming it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

if [[ -z "$CLICKHOUSE_GRPC_CLIENT" ]]; then
    CLICKHOUSE_GRPC_CLIENT="$CUR_DIR/../../../utils/grpc-client/clickhouse-grpc-client.py"
fi

U="u_grpc_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t_grpc (a UInt64, b String DEFAULT 'b_default') ENGINE = Memory"
printf '{"a":1,"%s":1}' "$U" | $CLICKHOUSE_GRPC_CLIENT --port "${CLICKHOUSE_PORT_GRPC:-9100}" --database "$CLICKHOUSE_DATABASE" \
    --query "INSERT INTO t_grpc FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS text_log"
$CLICKHOUSE_CLIENT -q "
    SELECT count(), countIf(position(message, ' in format JSONEachRow ') > 0) FROM system.text_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 1 HOUR AND level = 'Warning'
      AND position(message, 'Field \`${U}\` ') > 0"
$CLICKHOUSE_CLIENT -q "SELECT * FROM t_grpc"
