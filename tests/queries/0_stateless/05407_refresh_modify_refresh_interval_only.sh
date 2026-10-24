#!/usr/bin/env bash
# An `ALTER TABLE ... MODIFY REFRESH` that changes only the interval must reschedule the view:
# the refresh strategy's tree hash has to include the intervals, not only the node type.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS mv_05407;
    CREATE MATERIALIZED VIEW mv_05407 REFRESH EVERY 1 YEAR ENGINE = Memory EMPTY AS SELECT 1 AS x;
"

# The new schedule is applied by the refresh task's background scheduling, so wait for it.
wait_for_next_refresh_time()
{
    local condition=$1
    for _ in {1..600}
    do
        res=$($CLICKHOUSE_CLIENT -q "SELECT $condition FROM system.view_refreshes WHERE database = currentDatabase() AND view = 'mv_05407'")
        [ "$res" = "1" ] && break
        sleep 0.1
    done
    echo "$res"
}

wait_for_next_refresh_time "next_refresh_time > now() + INTERVAL 10 DAY"

$CLICKHOUSE_CLIENT -q "ALTER TABLE mv_05407 MODIFY REFRESH EVERY 1 DAY"

wait_for_next_refresh_time "next_refresh_time <= now() + INTERVAL 2 DAY"

$CLICKHOUSE_CLIENT -q "DROP TABLE mv_05407"
