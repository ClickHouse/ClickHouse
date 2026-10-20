#!/usr/bin/env bash
# Tags: atomic-database

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# After `refresh_retries` ran out, a view on a DEPENDS ON cycle retries the dependency refreshes it failed
# to process, but not before `refresh_retry_max_backoff_ms`. A negative RANDOMIZE FOR offset must not
# shorten that. Each member draws its own offset, so with 8 members some of them get a negative one.
# https://github.com/ClickHouse/ClickHouse/issues/123775

members=8

wait_for()
{
    local query=$1
    for _ in {1..600}
    do
        [ "$($CLICKHOUSE_CLIENT -q "$query")" == 1 ] && return
        sleep 0.1
    done
}

$CLICKHOUSE_CLIENT -q "create table fail (x UInt8) engine MergeTree order by x"
for i in $(seq 1 $members)
do
    # The aggregate keeps the throw at execution time.
    $CLICKHOUSE_CLIENT -q "
        create materialized view member$i refresh after 1 second randomize for 4 second depends on head
            settings refresh_retries = 0, refresh_retry_max_backoff_ms = 60000
            append (x Int64) engine MergeTree order by x
            as select toInt64(count()) + throwIf(count() > 0, 'boom') as x from fail"
done
$CLICKHOUSE_CLIENT -q "
    create materialized view head refresh every 1 second depends on $(seq -s ', ' -f 'member%g' 1 $members)
        append (x Int64) engine MergeTree order by x
        as select 1 as x;
    system refresh view head;"

wait_for "
    select countIf(last_success_time is not null) = $members from system.view_refreshes
    where database = currentDatabase() and view like 'member%'"
# Make all members fail on the same refresh of `head`: wait until they all processed its latest refresh.
$CLICKHOUSE_CLIENT -q "system stop view head"
wait_for "
    select countIf(status = 'WaitingForDependencies') = $members from system.view_refreshes
    where database = currentDatabase() and view like 'member%'"
$CLICKHOUSE_CLIENT -q "
    insert into fail values (1);
    system start view head;"
wait_for "
    select countIf(exception != '' and status = 'Scheduled' and next_refresh_time is not null) = $members
    from system.view_refreshes
    where database = currentDatabase() and view like 'member%'"

# The failed attempt ended at `last_refresh_time`, rounded down to seconds. The retry must wait
# for 1 second more (for the rounding) and the backoff.
$CLICKHOUSE_CLIENT -q "
    select 'retries wait for the backoff:', countIf(next_refresh_time >= last_refresh_time + 61) = $members
    from system.view_refreshes
    where database = currentDatabase() and view like 'member%'"

$CLICKHOUSE_CLIENT -q "drop table head"
for i in $(seq 1 $members)
do
    $CLICKHOUSE_CLIENT -q "drop table member$i"
done
$CLICKHOUSE_CLIENT -q "drop table fail"
