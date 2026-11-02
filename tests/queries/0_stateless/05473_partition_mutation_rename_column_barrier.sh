#!/usr/bin/env bash

# Regression test: a barrier `ALTER` (`RENAME COLUMN`) must wait for all earlier unfinished
# mutations, not only for the newest one. Partition-scoped (`IN PARTITION`) mutations can finish
# out of order: here the newest one targets an empty partition and is done immediately, while the
# older one is still pending in partition 1. If the rename does not wait for it, the values of the
# renamed column in partition 1 are lost when the pending mutation runs.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (p UInt16, n Int64, j Int64) ENGINE = MergeTree PARTITION BY p ORDER BY tuple();
    INSERT INTO t VALUES (1, 10, 123), (2, 20, 456);
    SYSTEM STOP MERGES t;
    ALTER TABLE t UPDATE n = n + 1 IN PARTITION 1 WHERE 1;
    ALTER TABLE t UPDATE n = n + 1 IN PARTITION 999 WHERE 1;
"

rename_query_id="${CLICKHOUSE_DATABASE}_rename_$RANDOM$RANDOM"
$CLICKHOUSE_CLIENT --query_id "$rename_query_id" -q "ALTER TABLE t RENAME COLUMN j TO k SETTINGS alter_sync = 0" &

# The rename must be blocked on the pending mutation of partition 1 while merges are stopped.
while true
do
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 't' AND name = 'k'")" = "1" ]; then
        echo "renamed before the pending mutation finished"
        break
    fi
    if [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.processes WHERE query_id = '$rename_query_id'")" = "1" ]; then
        break
    fi
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SYSTEM START MERGES t"
wait

$CLICKHOUSE_CLIENT -q "
    ALTER TABLE t UPDATE n = n WHERE 1 SETTINGS mutations_sync = 2;
    SELECT p, n, k FROM t ORDER BY p;
    DROP TABLE t;
"
