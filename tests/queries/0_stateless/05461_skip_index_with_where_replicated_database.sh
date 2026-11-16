#!/usr/bin/env bash
# Tags: zookeeper

# A `Replicated` database applies an `ALTER` without the user who submitted it,
# so the condition of a partial index is authorized before the query is replicated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="rdb_${CLICKHOUSE_TEST_UNIQUE_NAME}"
user="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

function cleanup()
{
    $CLICKHOUSE_CLIENT -q "DROP DATABASE IF EXISTS $db SYNC" >/dev/null 2>&1 ||:
    $CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS $user" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT -q "CREATE DATABASE $db ENGINE = Replicated('/test/${CLICKHOUSE_TEST_ZOOKEEPER_PREFIX}/$db', 's1', 'r1')"
$CLICKHOUSE_CLIENT --distributed_ddl_output_mode=none -q "
CREATE TABLE secret (sensitive UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE $db.tab (p UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE USER $user;
GRANT ALTER ON $db.* TO $user;
"

function run_as_user()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$user" --allow_experimental_partial_skip_indexes=1 --distributed_ddl_output_mode=none -q "$1" 2>&1)
    echo "$output" | grep -o -m1 -E "ACCESS_DENIED|QUERY_IS_PROHIBITED" || echo "no error"
}

predicate="p = intDiv(1, hasColumnInTable('$CLICKHOUSE_DATABASE', 'secret', 'sensitive'))"

echo "a condition the user may not run"
run_as_user "ALTER TABLE $db.tab ADD INDEX denied p TYPE minmax WHERE $predicate"
run_as_user "ALTER TABLE $db.tab ADD COLUMN c UInt8, DROP PARTITION ID 'none', ADD INDEX denied p TYPE minmax WHERE $predicate"

echo "a condition the user may run"
run_as_user "ALTER TABLE $db.tab ADD INDEX allowed p TYPE minmax WHERE p > 0"
run_as_user "ALTER TABLE $db.tab ADD COLUMN c UInt8, DROP PARTITION ID 'none', ADD INDEX allowed_too p TYPE minmax WHERE p > 0"

$CLICKHOUSE_CLIENT -q "SELECT name FROM system.data_skipping_indices WHERE database = '$db' ORDER BY name"
$CLICKHOUSE_CLIENT -q "SELECT name FROM system.columns WHERE database = '$db' ORDER BY name"
