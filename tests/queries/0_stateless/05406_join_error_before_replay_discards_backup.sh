#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel -- uses server-wide failpoints that would pause or break the persistent `Set` and
# `Join` inserts of concurrently running tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A persistent `INSERT` into a `Join` whose backup was already promoted, and which then fails before
# the replay of that backup starts (for example, it cannot take the write lock behind a long reader
# within `lock_acquire_timeout`), must discard the backup before it leaves the publish critical
# section. Otherwise a concurrent rebuild of the live state from the backup files, which can run
# before the sink's own cleanup (`onException`), publishes the rows of the failed insert.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS join_fail_before_replay;
    CREATE TABLE join_fail_before_replay (k UInt64, v String) ENGINE = Join(ANY, LEFT, k)
        SETTINGS max_rows_in_join = 2, join_overflow_mode = 'throw';
    INSERT INTO join_fail_before_replay VALUES (1, 'committed');
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_join_publish_fail_before_replay"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT set_or_join_sink_pause_before_discard"

# Promotes its backup, fails before replaying it, and stops before its own cleanup.
$CLICKHOUSE_CLIENT --query "INSERT INTO join_fail_before_replay VALUES (9, 'failed')" 2>&1 | grep -o -m1 'FAULT_INJECTED' &
failed_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT set_or_join_sink_pause_before_discard PAUSE"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_join_publish_fail_before_replay"

# Another insert exceeds `max_rows_in_join` and its rollback fails as well, so the live state is lost.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_join_publish_fail_during_rollback"
$CLICKHOUSE_CLIENT --query "INSERT INTO join_fail_before_replay VALUES (2, 'failed'), (3, 'failed')" 2>&1 | grep -o -m1 'SET_SIZE_LIMIT_EXCEEDED' > "$CLICKHOUSE_TMP/05406_rollback_out" &
rollback_pid=$!

for _ in {1..600}
do
    $CLICKHOUSE_CLIENT --query "SELECT count() FROM join_fail_before_replay" 2>&1 | grep -q 'NOT_INITIALIZED' && break
    sleep 0.1
done

# Rebuilds the lost live state from the backup files, while both failed inserts are still paused.
$CLICKHOUSE_CLIENT --query "OPTIMIZE TABLE join_fail_before_replay"

echo "rows after the rebuild:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_fail_before_replay ORDER BY k"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT set_or_join_sink_pause_before_discard"
wait $failed_pid
wait $rollback_pid
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_join_publish_fail_during_rollback"
cat "$CLICKHOUSE_TMP/05406_rollback_out"
rm -f "$CLICKHOUSE_TMP/05406_rollback_out"

$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_fail_before_replay;
    ATTACH TABLE join_fail_before_replay;
"
echo "rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_fail_before_replay ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE join_fail_before_replay"
