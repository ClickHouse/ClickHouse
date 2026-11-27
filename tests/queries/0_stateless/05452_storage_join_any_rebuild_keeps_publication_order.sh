#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The committed backups of a persistent `Join` are replayed in the order of their numbers, by the
# rollback of a failed `INSERT` and on restart. For `Join(ANY, ...)` this order decides which row of
# a duplicate key is kept, so it must be the order in which the inserts were published, not the
# order in which they started.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS join_publication_order;
    CREATE TABLE join_publication_order (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)
    SETTINGS join_any_take_last_row = 0, max_rows_in_join = 1, join_overflow_mode = 'throw';
"

# Insert `A` starts first, but publishes its row last: its `SELECT` is slow. It produces its only
# row after reading every row of `numbers`, so no filter can be pushed down into the source to skip
# the sleeps.
query_id_a="${CLICKHOUSE_DATABASE}_insert_a"
$CLICKHOUSE_CLIENT --query_id "$query_id_a" --max_block_size 1 --query "
    INSERT INTO join_publication_order SELECT 1, any('A') FROM numbers(20) WHERE sleepEachRow(0.5) = 0
" &
insert_a_pid=$!

# Wait until `A` is running, so that the backup number of its `INSERT` is already reserved.
while [[ $($CLICKHOUSE_CLIENT --query "SELECT count() FROM system.processes WHERE query_id = '$query_id_a' AND read_rows > 0") == 0 ]]
do
    if ! kill -0 $insert_a_pid 2>/dev/null
    then
        echo "Insert A finished before it was seen running"
        break
    fi
    sleep 0.1
done

# Insert `B` starts later, but publishes first, so its row is kept.
$CLICKHOUSE_CLIENT --query "INSERT INTO join_publication_order VALUES (1, 'B')"

wait $insert_a_pid

$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_publication_order ORDER BY k"

# The insert of a new key exceeds `max_rows_in_join`, and its rollback rebuilds the state from the
# committed backups. The value of the existing key must not change.
$CLICKHOUSE_CLIENT --query "INSERT INTO join_publication_order VALUES (2, 'C')" 2>&1 | grep -o -m1 'SET_SIZE_LIMIT_EXCEEDED'

$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_publication_order ORDER BY k"

# The same on restart.
$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_publication_order;
    ATTACH TABLE join_publication_order;
"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_publication_order ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE join_publication_order"
