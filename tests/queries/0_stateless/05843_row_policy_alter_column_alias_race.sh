#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database, no-fasttest
# no-parallel: the failpoint pauses the first ALTER through an alias on the server
# no-replicated-database: the DDL queue runs the ALTER of the target only after the paused ALTER
# an ALTER through an alias checks the row policies before the lock of the target, and an ALTER of the target between the two cannot break a policy

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB=$CLICKHOUSE_DATABASE
FP=storage_alias_alter_pause_before_target_lock

cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $FP"
    $CLICKHOUSE_CLIENT -q "DROP ROW POLICY IF EXISTS p ON $DB.t"
    $CLICKHOUSE_CLIENT -q "DROP ROW POLICY IF EXISTS p3 ON $DB.t"
}
trap cleanup EXIT

# runs $1 through the alias and $2 on the target while $1 waits after its check, before the lock of the target
race()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $FP"
    $CLICKHOUSE_CLIENT -q "$1" &
    $CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $FP PAUSE"
    $CLICKHOUSE_CLIENT --multiquery -q "$2" 2>&1 | grep -o ALTER_OF_COLUMN_IS_FORBIDDEN | head -1
    $CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $FP"
    wait
    $CLICKHOUSE_CLIENT -q "SELECT arrayStringConcat(groupArray(name), ',') FROM (SELECT name FROM system.columns WHERE database = currentDatabase() AND table = 't' ORDER BY position)"
}

$CLICKHOUSE_CLIENT --multiquery -q "
CREATE TABLE t (id UInt32, c1 UInt8, c2 UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 0, 1), (2, 0, 0);
CREATE TABLE a ENGINE = Alias(t);
CREATE ROW POLICY p ON $DB.t FOR SELECT USING c2 = 1 TO CURRENT_USER;
"

echo "-- the ALTER of the target checks the policies under its own lock, so it cannot move a policy column under the drop"
race "ALTER TABLE a DROP COLUMN c1" "ALTER TABLE t DROP COLUMN c1, RENAME COLUMN c2 TO c1"
$CLICKHOUSE_CLIENT -q "SELECT id FROM a"

echo "-- the commands prepared again under the lock of the target keep a skipped drop skipped"
race "ALTER TABLE a DROP COLUMN IF EXISTS c3" "ALTER TABLE t ADD COLUMN c3 UInt8; CREATE ROW POLICY p3 ON $DB.t FOR SELECT USING c3 = 0 TO CURRENT_USER"
$CLICKHOUSE_CLIENT -q "SELECT id FROM a"
