#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: tables of `Replicated` databases are not reattached.

# `reattach_tables_before_query_execution_probability` must not reattach a refreshable materialized view:
# `SYSTEM STOP VIEW` and `SYSTEM PAUSE VIEW` change the state of its refresh task, which a reattached view would lose,
# and it would start refreshing again.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function wait_for_status()
{
    local view=$1
    local status=$2
    while [[ $($CLICKHOUSE_CLIENT -q "SELECT status FROM system.view_refreshes WHERE database = currentDatabase() AND view = '$view'") != "$status" ]]
    do
        sleep 0.1
    done
}

for control in STOP PAUSE
do
    view="rmv_${control,,}"
    $CLICKHOUSE_CLIENT -q "CREATE MATERIALIZED VIEW $view REFRESH EVERY 1 DAY ENGINE = MergeTree ORDER BY x AS SELECT 1 AS x"
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT VIEW $view"
    $CLICKHOUSE_CLIENT -q "SYSTEM $control VIEW $view"
    wait_for_status "$view" Disabled

    $CLICKHOUSE_CLIENT -q "SELECT 1 FORMAT Null SETTINGS reattach_tables_before_query_execution_probability = 1"

    $CLICKHOUSE_CLIENT -q "SELECT '$control VIEW', status FROM system.view_refreshes WHERE database = currentDatabase() AND view = '$view'"
    $CLICKHOUSE_CLIENT -q "SELECT '$control VIEW', count() FROM $view"
done
