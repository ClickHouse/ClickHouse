#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel -- uses server-wide failpoints that would break or pause the persistent `Set` and
# `Join` inserts of concurrently running tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A persistent `INSERT` into a `Set` or `Join` that fails after its backup was promoted must get that
# backup out of the replay set before it leaves the publish critical section. The sink's own cleanup
# (`onException`) runs only after the critical section was released, so a concurrent `INSERT` that
# publishes in between must not replay the backup of the failed one.

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS set_race;
    CREATE TABLE set_race (k UInt64) ENGINE = Set SETTINGS persistent = 1;
    INSERT INTO set_race VALUES (1);
"

# The backup of the failed insert cannot be removed, so it is quarantined.
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_set_fail_during_backup_replay"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT set_or_join_sink_fail_removing_promoted_backup"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT set_or_join_sink_pause_before_discard"

$CLICKHOUSE_CLIENT --query "INSERT INTO set_race VALUES (2), (3)" 2>&1 | grep -o 'FAULT_INJECTED' | head -n 1 &
failed_insert_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT set_or_join_sink_pause_before_discard PAUSE"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_set_fail_during_backup_replay"

# Publishes a new `Set` replayed from all backups in the table directory.
$CLICKHOUSE_CLIENT --query "INSERT INTO set_race VALUES (4)"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT set_or_join_sink_pause_before_discard"
wait $failed_insert_pid
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT set_or_join_sink_fail_removing_promoted_backup"

echo "set rows after the concurrent insert:"
$CLICKHOUSE_CLIENT --query "SELECT groupArray(number) FROM numbers(10) WHERE number IN set_race"

$CLICKHOUSE_CLIENT --query "
    DETACH TABLE set_race;
    ATTACH TABLE set_race;
"
echo "set rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT groupArray(number) FROM numbers(10) WHERE number IN set_race"

$CLICKHOUSE_CLIENT --query "DROP TABLE set_race"

# The rollback of the failed `Join` insert fails as well, so the live state is marked as lost, and the
# concurrent insert rebuilds it from the backups in the table directory.
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS join_race;
    CREATE TABLE join_race (k UInt64, v String) ENGINE = Join(ALL, LEFT, k)
        SETTINGS max_rows_in_join = 3, join_overflow_mode = 'throw';
    INSERT INTO join_race VALUES (1, 'committed');
"

$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT storage_join_publish_fail_during_rollback"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT set_or_join_sink_pause_before_discard"

$CLICKHOUSE_CLIENT --query "INSERT INTO join_race VALUES (2, 'failed'), (3, 'failed'), (4, 'failed')" 2>&1 | grep -o 'SET_SIZE_LIMIT_EXCEEDED' | head -n 1 &
failed_insert_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT set_or_join_sink_pause_before_discard PAUSE"
$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT storage_join_publish_fail_during_rollback"

$CLICKHOUSE_CLIENT --query "INSERT INTO join_race VALUES (5, 'next')"

$CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT set_or_join_sink_pause_before_discard"
wait $failed_insert_pid

echo "join rows after the concurrent insert:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_race ORDER BY k"

$CLICKHOUSE_CLIENT --query "
    DETACH TABLE join_race;
    ATTACH TABLE join_race;
"
echo "join rows after reattach:"
$CLICKHOUSE_CLIENT --query "SELECT k, v FROM join_race ORDER BY k"

$CLICKHOUSE_CLIENT --query "DROP TABLE join_race"
