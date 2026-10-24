#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-ordinary-database
# no-parallel: `refresh_mv_pause_after_executor_published` is server-global and would park other refreshes.
# no-fasttest: a test enabling a fail point runs alone, and such tests are kept out of the fast test.
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.

# An `ALTER TABLE ... MODIFY QUERY` that changes only an operational setting of the stored query, such as
# `log_comment`, does not change what a refresh reads and produces. When it races with a running
# `REFRESH ... IF CHANGED APPEND`, that refresh must keep its watermark: otherwise the next refresh sees
# the unchanged source as changed and appends the same rows again.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP="refresh_mv_pause_after_executor_published"

trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT '"$FP"'" 2>/dev/null || true
' EXIT

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS mv_05445 SYNC;
    DROP TABLE IF EXISTS dst_05445 SYNC;
    DROP TABLE IF EXISTS src_05445 SYNC;
    CREATE TABLE src_05445 (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE dst_05445 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src_05445 VALUES (1);
    CREATE MATERIALIZED VIEW mv_05445 REFRESH EVERY 1 SECOND IF CHANGED APPEND TO dst_05445
        AS SELECT x FROM src_05445;
"

dst()
{
    $CLICKHOUSE_CLIENT -q "SELECT groupArray(x) FROM (SELECT x FROM dst_05445 ORDER BY x)"
}

wait_for_dst()
{
    local expected=$1
    local v=''
    for _ in {1..240}
    do
        v=$(dst)
        [ "$v" = "$expected" ] && break
        sleep 0.5
    done
    echo "$v"
}

echo "initial: $(wait_for_dst '[1]')"

# Park the scheduled refresh that appends the changed source.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"
$CLICKHOUSE_CLIENT -q "INSERT INTO src_05445 VALUES (2)"
timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE" || echo "the refresh did not reach $FP"

$CLICKHOUSE_CLIENT -q "ALTER TABLE mv_05445 MODIFY QUERY SELECT x FROM src_05445 SETTINGS log_comment = '05445'"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"
echo "appended: $(wait_for_dst '[1,1,2]')"

# The source has not changed since the parked refresh appended it, so the following scheduled refreshes
# append nothing.
sleep 3
echo "after: $(dst)"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv_05445 SYNC;
    DROP TABLE dst_05445 SYNC;
    DROP TABLE src_05445 SYNC;
"
