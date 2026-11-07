#!/usr/bin/env bash
# the blending engine check must not tell a user the engine of a hidden table or the name of a hidden policy

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PUBLIC="${CLICKHOUSE_DATABASE}_public"
DB_PRIVATE="${CLICKHOUSE_DATABASE}_private"
USER_POLICY="u_policy_${CLICKHOUSE_DATABASE}"
USER_TABLE="u_table_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP USER IF EXISTS $USER_POLICY, $USER_TABLE;
CREATE DATABASE $DB_PUBLIC;
CREATE DATABASE $DB_PRIVATE;
CREATE TABLE $DB_PRIVATE.src (k UInt64, v UInt64) ENGINE = SummingMergeTree ORDER BY k;
CREATE TABLE $DB_PUBLIC.a ENGINE = Alias('$DB_PRIVATE', 'src');
CREATE TABLE $DB_PUBLIC.a_later ENGINE = Alias('$DB_PRIVATE', 'later');
CREATE TABLE $DB_PUBLIC.m (k UInt64, v UInt64) ENGINE = Merge('$DB_PRIVATE', '^later_merge\$');
CREATE VIEW $DB_PUBLIC.w AS SELECT * FROM $DB_PRIVATE.src;
CREATE USER $USER_POLICY;
GRANT CREATE ROW POLICY ON $DB_PUBLIC.a TO $USER_POLICY;
GRANT CREATE ROW POLICY ON $DB_PUBLIC.a_later TO $USER_POLICY;
GRANT CREATE ROW POLICY ON $DB_PUBLIC.m TO $USER_POLICY;
GRANT CREATE ROW POLICY ON $DB_PUBLIC.w TO $USER_POLICY;
CREATE ROW POLICY classified_policy ON $DB_PUBLIC.future USING 1 TO ALL;
CREATE USER $USER_TABLE;
GRANT CREATE TABLE ON $DB_PUBLIC.future TO $USER_TABLE;
GRANT TABLE ENGINE ON SummingMergeTree TO $USER_TABLE;
"

echo "policy on an alias of a hidden table"
$CLICKHOUSE_CLIENT --user "$USER_POLICY" -q "CREATE ROW POLICY p ON $DB_PUBLIC.a USING 1 TO ALL" 2>&1 | grep -o -m1 -E "check the engine|SummingMergeTree"

# the answer must not depend on whether the hidden table exists
echo "policy on an alias of a hidden table which does not exist"
$CLICKHOUSE_CLIENT --user "$USER_POLICY" -q "CREATE ROW POLICY p ON $DB_PUBLIC.a_later USING 1 TO ALL" 2>&1 | grep -o -m1 -E "check the engine"
$CLICKHOUSE_CLIENT -q "CREATE TABLE $DB_PRIVATE.later (k UInt64) ENGINE = MergeTree ORDER BY k"
echo "policy on an alias of a hidden table which exists"
$CLICKHOUSE_CLIENT --user "$USER_POLICY" -q "CREATE ROW POLICY p ON $DB_PUBLIC.a_later USING 1 TO ALL" 2>&1 | grep -o -m1 -E "check the engine"
echo "policy on a merge table which can read hidden tables"
$CLICKHOUSE_CLIENT --user "$USER_POLICY" -q "CREATE ROW POLICY p ON $DB_PUBLIC.m USING 1 TO ALL" 2>&1 | grep -o -m1 -E "check the engine"
echo "policy on a view of a hidden table"
$CLICKHOUSE_CLIENT --user "$USER_POLICY" -q "CREATE ROW POLICY p ON $DB_PUBLIC.w USING 1 TO ALL" 2>&1 | grep -o -m1 -E "check the engine|SummingMergeTree"

echo "table under a hidden policy"
$CLICKHOUSE_CLIENT --user "$USER_TABLE" -q "CREATE TABLE $DB_PUBLIC.future (k UInt64, v UInt64) ENGINE = SummingMergeTree ORDER BY k" 2>&1 | grep -o -m1 -E "classified_policy|BAD_ARGUMENTS"

$CLICKHOUSE_CLIENT -m -q "
DROP ROW POLICY classified_policy ON $DB_PUBLIC.future;
DROP USER $USER_POLICY, $USER_TABLE;
DROP DATABASE $DB_PUBLIC;
DROP DATABASE $DB_PRIVATE;
"
