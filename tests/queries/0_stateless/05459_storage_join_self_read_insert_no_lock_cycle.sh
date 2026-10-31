#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel -- uses a server-wide failpoint that would pause the persistent `Set`/`Join`
# inserts of concurrently running tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A persistent `Join` insert publishes its backup holding `mutate_mutex` and waits for the write
# lock. An `INSERT INTO j SELECT * FROM j` holds the read lock of its source, so it must be rejected
# before it waits on `mutate_mutex`, otherwise neither insert can proceed.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS join_self_read;
    CREATE TABLE join_self_read (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
    INSERT INTO join_self_read VALUES (1, 'a');
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT set_or_join_sink_pause_before_replay"

# Pauses holding `mutate_mutex`. Without a timeout it would wait for the write lock forever.
$CLICKHOUSE_CLIENT --query "INSERT INTO join_self_read SETTINGS lock_acquire_timeout = 0 VALUES (2, 'b')" &
insert_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT set_or_join_sink_pause_before_replay PAUSE"

# Holds the read lock of its source and fails right away instead of waiting on `mutate_mutex`.
$CLICKHOUSE_CLIENT --query "INSERT INTO join_self_read SELECT k + 10, v FROM join_self_read" 2>&1 | grep -o -m1 'DEADLOCK_AVOIDED'

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT set_or_join_sink_pause_before_replay"

wait $insert_pid

$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_self_read ORDER BY k"
$CLICKHOUSE_CLIENT --query "DROP TABLE join_self_read"
