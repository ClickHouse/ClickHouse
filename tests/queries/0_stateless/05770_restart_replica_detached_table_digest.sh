#!/usr/bin/env bash
# Tags: zookeeper, no-shared-merge-tree
# Tag no-shared-merge-tree: SYSTEM RESTART REPLICA needs a ReplicatedMergeTree table.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_repl"
ZK_PATH="/test/restart_replica_detached_table_digest/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "CREATE DATABASE ${DB} ENGINE = Replicated('${ZK_PATH}', 's1', 'r1')"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none --enable_time_series_aggregate_functions=1 -q "
    CREATE TABLE ${DB}.t (k UInt64, agg AggregateFunction(timeSeriesLastToGrid(100, 200, 10, 15), DateTime('UTC'), Float64))
    ENGINE = ReplicatedAggregatingMergeTree ORDER BY k"

# Without the setting the stored definition cannot be re-created, so RESTART REPLICA fails after detaching the table.
${CLICKHOUSE_CLIENT} --enable_time_series_aggregate_functions=0 -q "SYSTEM RESTART REPLICA ${DB}.t" 2>&1 | grep -o -m1 UNKNOWN_AGGREGATE_FUNCTION
${CLICKHOUSE_CLIENT} -q "EXISTS TABLE ${DB}.t"

# Each DDL writes the database's in-memory metadata digest to Keeper.
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "CREATE TABLE ${DB}.t2 (x UInt64) ENGINE = ReplicatedMergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode=none -q "DROP TABLE ${DB}.t2 SYNC"

# No table is attached any more, so the digest must be 0.
${CLICKHOUSE_CLIENT} -q "SELECT value FROM system.zookeeper WHERE path = '${ZK_PATH}/replicas/s1|r1' AND name = 'digest'"

${CLICKHOUSE_CLIENT} -q "DROP DATABASE ${DB} SYNC"
