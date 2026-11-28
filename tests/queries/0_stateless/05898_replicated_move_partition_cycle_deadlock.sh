#!/usr/bin/env bash
# Tags: zookeeper, long

# Concurrent MOVE PARTITION TO TABLE between ReplicatedMergeTree tables in a cycle must not deadlock on their parts locks.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

for t in t_a t_b t_c; do
    $CLICKHOUSE_CLIENT -q "CREATE TABLE $t (p UInt64, k UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/$t', '1') PARTITION BY p ORDER BY k"
done

$CLICKHOUSE_CLIENT -q "INSERT INTO t_a SELECT 1 + number % 60, number FROM numbers(600)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_b SELECT 101 + number % 60, number FROM numbers(600)"
$CLICKHOUSE_CLIENT -q "INSERT INTO t_c SELECT 201 + number % 60, number FROM numbers(600)"

# move_thread <src> <dst> <partition offset> <first p>: moves partitions offset+p for p = first, first+3, ... <= 60
function move_thread()
{
    local TIMELIMIT=$((SECONDS+TIMEOUT))
    for p in $(seq "$4" 3 60)
    do
        [ $SECONDS -lt "$TIMELIMIT" ] || break
        $CLICKHOUSE_CLIENT --alter_sync 0 -q "ALTER TABLE $1 MOVE PARTITION $(($3 + p)) TO TABLE $2"
    done
}

TIMEOUT=20

for i in 1 2 3; do
    move_thread t_c t_a 200 $i 2>/dev/null &
    move_thread t_a t_b 0 $i 2>/dev/null &
    move_thread t_b t_a 100 $i 2>/dev/null &
done

wait

for t in t_a t_b t_c; do
    $CLICKHOUSE_CLIENT -q "DROP TABLE $t"
done

echo 'ok'
