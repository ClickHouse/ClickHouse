#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest, zookeeper
# no-parallel: the failpoint is server-global and would park every other refresh on the server.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none"
FP="refresh_mv_pause_before_exchange"
trap '$CLICKHOUSE_CLIENT -q "system disable failpoint $FP" 2>/dev/null' EXIT

# Parks a refresh of `mv` after it has filled its temporary table, replaces the table under the temporary
# name ($2 runs additional queries), and lets the refresh continue to the exchange.
refresh_with_replaced_temp_table() {
    local db=$1
    $CLICKHOUSE_CLIENT -q "
        system enable failpoint $FP;
        system refresh view $db.mv;"
    timeout 60 $CLICKHOUSE_CLIENT -q "system wait failpoint $FP pause" || echo "FAIL: the refresh did not reach $FP"
    local uuid
    uuid=$($CLICKHOUSE_CLIENT -q "select uuid from system.tables where database = '$db' and name = 'mv'")
    $CLICKHOUSE_CLIENT -q "
        create or replace table $db.\`.tmp.inner_id.$uuid\` (x Int64) engine Memory;
        $2"
    $CLICKHOUSE_CLIENT -q "system disable failpoint $FP"
    $CLICKHOUSE_CLIENT -q "system wait view $db.mv" 2>&1 | grep -o -m1 "TABLE_UUID_MISMATCH"
}

for engine in "Atomic" "Replicated('/test/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/rdb', 's1', 'r1')"
do
    # A `Replicated` database accepts such a view only with a replicated target.
    [ "$engine" = "Atomic" ] && table_engine="MergeTree" || table_engine="ReplicatedMergeTree"
    db="${CLICKHOUSE_DATABASE}_db"
    echo "${engine%%(*}"
    $CLICKHOUSE_CLIENT -q "
        create database $db engine = $engine;
        create table $db.src (x Int64) engine MergeTree order by x;
        create table $db.tgt (x Int64) engine $table_engine order by x;
        insert into $db.src values (1);
        create materialized view $db.mv refresh every 1 year settings refresh_retries = 0 to $db.tgt empty as
            select x from $db.src;"

    refresh_with_replaced_temp_table "$db"
    $CLICKHOUSE_CLIENT -q "select engine = '$table_engine', total_rows from system.tables where database = '$db' and name = 'tgt'"

    # The next refresh publishes its own table, with the engine of the target.
    $CLICKHOUSE_CLIENT -q "
        system refresh view $db.mv;
        system wait view $db.mv;
        select engine = '$table_engine', total_rows from system.tables where database = '$db' and name = 'tgt';"

    # Without the target the refresh renames instead of exchanging.
    refresh_with_replaced_temp_table "$db" "drop table $db.tgt;"
    $CLICKHOUSE_CLIENT -q "select count() from system.tables where database = '$db' and name = 'tgt'"

    $CLICKHOUSE_CLIENT -q "drop database $db sync"
done
