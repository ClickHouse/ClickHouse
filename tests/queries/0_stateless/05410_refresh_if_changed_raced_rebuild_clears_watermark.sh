#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, no-replicated-database, no-ordinary-database
# no-parallel: `refresh_mv_pause_before_exchange` is server-global and would park other refreshes.
# no-fasttest: a test enabling a fail point runs alone, and such tests are kept out of the fast test.
# Refreshable MVs with non-replicated inner tables are refused on a Replicated database.

# A refresh that raced with an `ALTER ... MODIFY QUERY` still publishes the target it built from the
# old definition, so it must clear the persisted `REFRESH ... IF CHANGED` watermark. Otherwise, when the
# query is changed back while that refresh is in flight, the next scheduled refresh sees the current
# definition and the unchanged source match the old watermark, skips, and leaves the rows of the
# definition that is no longer current in the view.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FP="refresh_mv_pause_before_exchange"

trap '
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT '"$FP"'" 2>/dev/null || true
' EXIT

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS mv_05410 SYNC;
    DROP TABLE IF EXISTS src_05410 SYNC;
    CREATE TABLE src_05410 (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src_05410 VALUES (1);
    CREATE MATERIALIZED VIEW mv_05410 REFRESH EVERY 1 SECOND IF CHANGED
        ENGINE = MergeTree ORDER BY x AS SELECT x FROM src_05410;
"

wait_for_value()
{
    local expected=$1
    local v=''
    for _ in {1..240}
    do
        v=$($CLICKHOUSE_CLIENT -q "SELECT groupArray(x) FROM mv_05410")
        [ "$v" = "$expected" ] && break
        sleep 0.5
    done
    echo "$v"
}

target_uuid()
{
    $CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner%'"
}

echo "definition A: $(wait_for_value '[1]')"

# Park the next refresh, which runs definition B, right before it exchanges the target.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"
$CLICKHOUSE_CLIENT -q "ALTER TABLE mv_05410 MODIFY QUERY SELECT x + 100 AS x FROM src_05410"
timeout 60 $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE" || echo "the refresh did not reach $FP"
# B is parked before the exchange, so this is still the target that B replaces.
uuid_before_b=$(target_uuid)

# Change the query back to A while B is in flight, then let B publish its target.
$CLICKHOUSE_CLIENT -q "ALTER TABLE mv_05410 MODIFY QUERY SELECT x FROM src_05410"
$CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"

# Disabling the fail point only wakes the refresh, so wait until B has exchanged the target. Otherwise
# the old target still holds `[1]`, and the check below would pass before B has published anything.
for _ in {1..240}
do
    [ "$(target_uuid)" != "$uuid_before_b" ] && break
    sleep 0.5
done
[ "$(target_uuid)" != "$uuid_before_b" ] || echo "the parked refresh did not publish its target"

# The source never changed, but the following scheduled refresh must rebuild the view under A.
echo "definition A again: $(wait_for_value '[1]')"

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv_05410 SYNC;
    DROP TABLE src_05410 SYNC;
"
