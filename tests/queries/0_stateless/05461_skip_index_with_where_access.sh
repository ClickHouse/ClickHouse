#!/usr/bin/env bash
# Tags: no-random-settings, no-parallel

# The condition and the indexed expression of a partial index are analysed with the privileges of the server, so they must be
# authorized first as the user who defines the index: a user must not probe tables and columns that they can't see, even with
# the feature disabled.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05461_${CLICKHOUSE_DATABASE}"
udf="udf_05461_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP USER IF EXISTS $user;
DROP FUNCTION IF EXISTS $udf;
DROP TABLE IF EXISTS secret;
DROP TABLE IF EXISTS scratch;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS hidden;
CREATE TABLE secret (sensitive UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE scratch (p UInt8, INDEX i p TYPE minmax) ENGINE = MergeTree ORDER BY tuple();
CREATE FUNCTION $udf AS (t, c) -> hasColumnInTable('$CLICKHOUSE_DATABASE', t, c);
CREATE USER $user;
GRANT CREATE TABLE, ALTER ADD INDEX ON $CLICKHOUSE_DATABASE.* TO $user;
GRANT TABLE ENGINE ON MergeTree TO $user;
"

function run_as_user()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "$user" --allow_experimental_partial_skip_indexes="$2" -q "$1" 2>&1)
    echo "$output" | grep -o -m1 -E "ACCESS_DENIED|UNKNOWN_TABLE|BAD_ARGUMENTS|ILLEGAL_DIVISION|SUPPORT_IS_DISABLED" || echo "no error"
}

echo "the user can't see the table"
run_as_user "SELECT hasColumnInTable('$CLICKHOUSE_DATABASE', 'secret', 'sensitive')" 1

for enabled in 1 0
do
    echo "feature enabled: $enabled"
    for column in sensitive absent
    do
        for table in secret absent_table
        do
            predicate="p = intDiv(1, hasColumnInTable('$CLICKHOUSE_DATABASE', '$table', '$column'))"
            run_as_user "CREATE TABLE ${CLICKHOUSE_DATABASE}.probe (p UInt8, INDEX i p TYPE minmax WHERE $predicate) ENGINE = MergeTree ORDER BY tuple()" $enabled
            run_as_user "ALTER TABLE ${CLICKHOUSE_DATABASE}.scratch ADD INDEX j p TYPE minmax WHERE $predicate" $enabled
            run_as_user "CREATE INDEX j ON ${CLICKHOUSE_DATABASE}.scratch (p) TYPE minmax WHERE $predicate" $enabled
            expression="p + intDiv(1, hasColumnInTable('$CLICKHOUSE_DATABASE', '$table', '$column'))"
            run_as_user "CREATE TABLE ${CLICKHOUSE_DATABASE}.probe (p UInt8, INDEX i $expression TYPE minmax WHERE p > 0) ENGINE = MergeTree ORDER BY tuple()" $enabled
            run_as_user "ALTER TABLE ${CLICKHOUSE_DATABASE}.scratch ADD INDEX j $expression TYPE minmax WHERE p > 0" $enabled
            run_as_user "CREATE INDEX j ON ${CLICKHOUSE_DATABASE}.scratch ($expression) TYPE minmax WHERE p > 0" $enabled
        done
    done
    run_as_user "CREATE TABLE ${CLICKHOUSE_DATABASE}.probe (p UInt8, INDEX i p TYPE minmax WHERE p = intDiv(1, $udf('secret', 'sensitive'))) ENGINE = MergeTree ORDER BY tuple()" $enabled
    run_as_user "ALTER TABLE ${CLICKHOUSE_DATABASE}.scratch ADD INDEX j p TYPE minmax WHERE p = intDiv(1, $udf('secret', 'sensitive'))" $enabled
    run_as_user "CREATE TABLE ${CLICKHOUSE_DATABASE}.probe (p UInt8, INDEX i p + intDiv(1, $udf('secret', 'sensitive')) TYPE minmax WHERE p > 0) ENGINE = MergeTree ORDER BY tuple()" $enabled
    run_as_user "ALTER TABLE ${CLICKHOUSE_DATABASE}.scratch ADD INDEX j p + intDiv(1, $udf('secret', 'sensitive')) TYPE minmax WHERE p > 0" $enabled
done

echo "a function the server context resolves to another object"
# The session database is the test one, the server context's is `default`: the user may see `hidden` only in the former.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE hidden (sensitive UInt8) ENGINE = MergeTree ORDER BY tuple();
GRANT SHOW COLUMNS ON $CLICKHOUSE_DATABASE.hidden TO $user;
"
run_as_user "SELECT hasColumnInTable(currentDatabase(), 'hidden', 'sensitive')" 1
for enabled in 1 0
do
    predicate="p = intDiv(1, hasColumnInTable(currentDatabase(), 'hidden', 'sensitive'))"
    expression="p + intDiv(1, hasColumnInTable(currentDatabase(), 'hidden', 'sensitive'))"
    run_as_user "CREATE TABLE ${CLICKHOUSE_DATABASE}.probe (p UInt8, INDEX i p TYPE minmax WHERE $predicate) ENGINE = MergeTree ORDER BY tuple()" $enabled
    run_as_user "ALTER TABLE ${CLICKHOUSE_DATABASE}.scratch ADD INDEX j p TYPE minmax WHERE $predicate" $enabled
    run_as_user "CREATE INDEX j ON ${CLICKHOUSE_DATABASE}.scratch (p) TYPE minmax WHERE $predicate" $enabled
    run_as_user "CREATE TABLE ${CLICKHOUSE_DATABASE}.probe (p UInt8, INDEX i $expression TYPE minmax WHERE p > 0) ENGINE = MergeTree ORDER BY tuple()" $enabled
    run_as_user "ALTER TABLE ${CLICKHOUSE_DATABASE}.scratch ADD INDEX j $expression TYPE minmax WHERE p > 0" $enabled
    run_as_user "CREATE INDEX j ON ${CLICKHOUSE_DATABASE}.scratch ($expression) TYPE minmax WHERE p > 0" $enabled
done

echo "introspection function in the indexed expression"
output=$($CLICKHOUSE_CLIENT --user "$user" --allow_experimental_partial_skip_indexes=1 --allow_introspection_functions=1 \
    -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.probe (p UInt8, INDEX i demangle(toString(p)) TYPE minmax WHERE p > 0) ENGINE = MergeTree ORDER BY tuple()" 2>&1)
echo "$output" | grep -o -m1 "ACCESS_DENIED" || echo "no error"

echo "copied from another table"
$CLICKHOUSE_CLIENT --allow_experimental_partial_skip_indexes=1 --allow_introspection_functions=1 -q "
CREATE TABLE src (p UInt8, INDEX i p TYPE minmax WHERE demangle(toString(p)) != '') ENGINE = MergeTree ORDER BY tuple();
GRANT SHOW COLUMNS, SELECT ON $CLICKHOUSE_DATABASE.src TO $user;
GRANT INSERT, ALTER ON $CLICKHOUSE_DATABASE.probe TO $user;
"
for copy in "AS" "CLONE AS"
do
    output=$($CLICKHOUSE_CLIENT --user "$user" --allow_experimental_partial_skip_indexes=1 --allow_introspection_functions=1 \
        -q "CREATE TABLE ${CLICKHOUSE_DATABASE}.probe $copy ${CLICKHOUSE_DATABASE}.src" 2>&1)
    echo "$output" | grep -o -m1 "ACCESS_DENIED" || echo "no error"
    $CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS probe"
done

echo "short attach of a stored table"
$CLICKHOUSE_CLIENT -q "GRANT CREATE TABLE ON $CLICKHOUSE_DATABASE.src TO $user"
$CLICKHOUSE_CLIENT -q "DETACH TABLE src"
$CLICKHOUSE_CLIENT --user "$user" --allow_experimental_partial_skip_indexes=1 --allow_introspection_functions=1 \
    -q "ATTACH TABLE ${CLICKHOUSE_DATABASE}.src" 2>&1 | grep -o -m1 "ACCESS_DENIED" || echo "no error"
$CLICKHOUSE_CLIENT -q "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'src'"

$CLICKHOUSE_CLIENT -q "
DROP USER $user;
DROP FUNCTION $udf;
DROP TABLE secret;
DROP TABLE scratch;
DROP TABLE src;
DROP TABLE hidden;
DROP TABLE IF EXISTS probe;
"
