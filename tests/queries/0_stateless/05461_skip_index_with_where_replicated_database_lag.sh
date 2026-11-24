#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-parallel
# no-parallel: the `database_replicated_pause_before_initial_query_execution` failpoint fires globally.

# A `Replicated` database runs an `ALTER` on the host it is submitted to only after the entries queued before it,
# and one of them can change what an alias in the condition of a partial index expands to. So the host runs the
# `ALTER` as the user who submitted it, not only checks it against the schema it had before.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="rdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"
zk_path="/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/$db"
failpoint="database_replicated_pause_before_initial_query_execution"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "SYSTEM DISABLE FAILPOINT $failpoint" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db SYNC" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "CREATE DATABASE $db ENGINE = Replicated('$zk_path', 's1', 'r1')"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "
CREATE TABLE secret (sensitive UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE $db.tab (p UInt8, a UInt8 ALIAS 1) ENGINE = MergeTree ORDER BY tuple();
CREATE USER $user;
GRANT ALTER ON $db.* TO $user;
"

function queued_entries()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.zookeeper WHERE path = '$zk_path/log'"
}

# An earlier entry makes the alias read a table the user can't see, and is held before it runs.
$CLICKHOUSE_CLIENT -q "SYSTEM ENABLE FAILPOINT $failpoint"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none \
    -q "ALTER TABLE $db.tab MODIFY COLUMN a UInt8 ALIAS hasColumnInTable('$CLICKHOUSE_DATABASE', 'secret', 'sensitive')" &
alter_pid=$!
$CLICKHOUSE_CLIENT -q "SYSTEM WAIT FAILPOINT $failpoint PAUSE"

# Checked here against `a ALIAS 1`, the index is queued after that entry.
entries=$(queued_entries)
$CLICKHOUSE_CLIENT --user "$user" --allow_experimental_partial_skip_indexes=1 --distributed_ddl_output_mode=none \
    -q "ALTER TABLE $db.tab ADD INDEX i p TYPE minmax WHERE p = intDiv(1, a)" 2>&1 \
    | grep -o -m1 -E "ACCESS_DENIED|BAD_ARGUMENTS|ILLEGAL_DIVISION" || echo "no error" &
index_pid=$!
for _ in $(seq 1 300); do
    [ "$(queued_entries)" -gt "$entries" ] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT -q "SYSTEM NOTIFY FAILPOINT $failpoint"
wait $alter_pid
wait $index_pid

$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.data_skipping_indices WHERE database = '$db'"
