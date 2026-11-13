#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-shared-merge-tree, no-replicated-database
# Tag no-fasttest: requires S3
# Tag no-parallel: the failpoint is server-global and breaks every plain_rewritable directory move
# Tag no-shared-merge-tree: checks the MergeTree retry
# Tag no-replicated-database: plain rewritable should not be shared between replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

trap '$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_move"' EXIT

ERRORS='TIMEOUT_EXCEEDED\|QUERY_WAS_CANCELLED\|PART_IS_TEMPORARILY_LOCKED'

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t SYNC;
    CREATE TABLE t (n UInt64) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(
        name = '05620_${CLICKHOUSE_DATABASE}',
        type = s3_plain_rewritable,
        endpoint = 'http://localhost:11111/test/05620/${CLICKHOUSE_DATABASE}/',
        access_key_id = clickhouse,
        secret_access_key = clickhouse);
    INSERT INTO t VALUES (1), (2);
    SYSTEM ENABLE FAILPOINT plain_object_storage_write_fail_on_directory_move;
"

# The merged part all_1_1_1 cannot be renamed, so it stays outdated, and it cannot be removed either.
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t FINAL SETTINGS optimize_throw_if_noop = 1" 2>&1 | grep -o -m1 'FAULT_INJECTED'
$CLICKHOUSE_CLIENT -q "SELECT name, active FROM system.parts WHERE database = currentDatabase() AND table = 't' AND _state != '' ORDER BY name"

# The empty part made by TRUNCATE is all_1_1_1 too, so TRUNCATE retries until the outdated part is removed.
$CLICKHOUSE_CLIENT --max_execution_time 1 -q "TRUNCATE TABLE t" 2>&1 | grep -o -m1 "$ERRORS"
# DROP PARTITION uses the same retry, and it stops with timeout_overflow_mode = 'break' too: there is no partial result.
$CLICKHOUSE_CLIENT --max_execution_time 1 --timeout_overflow_mode break -q "ALTER TABLE t DROP PARTITION ID 'all'" 2>&1 | grep -o -m1 "$ERRORS"

query_id="05620_${CLICKHOUSE_DATABASE}_$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$query_id" -q "TRUNCATE TABLE t" 2>&1 | grep -o -m1 "$ERRORS" &
pid=$!
for _ in {1..300}
do
    [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$query_id'") == 1 ]] && break
    sleep 0.1
done
$CLICKHOUSE_CLIENT -q "KILL QUERY WHERE query_id = '$query_id' ASYNC" > /dev/null
timeout 30 tail --pid="$pid" -f /dev/null || echo "TRUNCATE ignored KILL QUERY"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT plain_object_storage_write_fail_on_directory_move"
wait

# The cancelled TRUNCATEs changed nothing, and the retry still lets TRUNCATE succeed once the part can be removed.
$CLICKHOUSE_CLIENT -m -q "
    SELECT count(), sum(n) FROM t;
    TRUNCATE TABLE t;
    SELECT count() FROM t;
"

# Filter out 'Removing temporary directory' because the fault injection prevents directory rename.
$CLICKHOUSE_CLIENT -q "DROP TABLE t SYNC" 2>&1 | grep -v 'Removing temporary directory' ||:
