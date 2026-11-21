#!/usr/bin/env bash
# Tags: zookeeper, no-parallel, no-fasttest, no-replicated-database
# Tag no-parallel: the failpoint pauses any `SYSTEM RESTART REPLICA` on the server.
# Tag no-replicated-database: the failpoint is enabled on one server only.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT restart_replica_pause_after_detach" ||:' EXIT

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src (x Int64) ENGINE = MergeTree ORDER BY x;
    CREATE MATERIALIZED VIEW mv ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/mv', 'r1') ORDER BY x
        AS SELECT x FROM src;
    INSERT INTO src SELECT number FROM numbers(1000);
"
inner=$($CLICKHOUSE_CLIENT -q "SELECT name FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner_id.%'")
[ -n "$inner" ] || { echo "No inner table of mv"; exit 1; }

$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT restart_replica_pause_after_detach"
$CLICKHOUSE_CLIENT -q "SYSTEM RESTART REPLICA \`$inner\`" &
restart_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT restart_replica_pause_after_detach PAUSE"

# The inner table is detached now. The replacement must still check its size, and not drop it with the limit switched off.
$CLICKHOUSE_CLIENT --max_table_size_to_drop 1 -q "
    CREATE OR REPLACE MATERIALIZED VIEW mv ENGINE = ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/mv2', 'r1') ORDER BY x
        AS SELECT x FROM src" 2>&1 | grep -o "TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT" | head -1 &
replace_pid=$!
# A replacement that does not wait for the restart finishes here, and drops the inner table after the restart.
for _ in {1..30}; do kill -0 $replace_pid 2>/dev/null || break; sleep 0.1; done

$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT restart_replica_pause_after_detach"
wait $restart_pid $replace_pid
$CLICKHOUSE_CLIENT -q "SELECT count() FROM mv"
