#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A mutation with several commands is listed once per command. It is sized once, and every row
# reports the same `bytes_to_do` and `progress`.

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_per_command_rows SYNC;
    CREATE TABLE t_per_command_rows (k UInt64, v UInt64, w UInt64)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_per_command_rows', 'r1') ORDER BY k;
    SYSTEM STOP MERGES t_per_command_rows;
    INSERT INTO t_per_command_rows SELECT number, 1, 1 FROM numbers(1000);
    INSERT INTO t_per_command_rows SELECT number, 2, 2 FROM numbers(1000);
    ALTER TABLE t_per_command_rows UPDATE v = v + 1 WHERE 1, UPDATE w = w + 1 WHERE 1 SETTINGS mutations_sync = 0;
"

# The mutation reaches the queue asynchronously.
rows=""
for _ in {1..300}; do
    rows=$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_per_command_rows' AND parts_to_do = 2")
    [ "$rows" = "2" ] && break
    sleep 0.2
done
[ "$rows" = "2" ] || echo "FAIL: the mutation never reached the queue with both parts"

$CLICKHOUSE_CLIENT -q "SELECT count(), uniqExact(bytes_to_do), min(bytes_to_do) > 0, groupUniqArray(progress) FROM system.mutations
    WHERE database = currentDatabase() AND table = 't_per_command_rows'"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_per_command_rows SYNC"
