#!/usr/bin/env bash
# the blending engine check must not run a table function which can read a hidden table

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_PUBLIC="${CLICKHOUSE_DATABASE}_public"
DB_PRIVATE="${CLICKHOUSE_DATABASE}_private"
USER_POLICY="u_policy_${CLICKHOUSE_DATABASE}"
FILE_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}.csv"

$CLICKHOUSE_CLIENT -m -q "
DROP USER IF EXISTS $USER_POLICY;
CREATE DATABASE $DB_PUBLIC;
CREATE DATABASE $DB_PRIVATE;
CREATE TABLE $DB_PRIVATE.src (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE $DB_PUBLIC.tf AS view(SELECT * FROM $DB_PRIVATE.src);
INSERT INTO FUNCTION file('$FILE_NAME', 'CSV', 'k UInt64') SELECT 1 SETTINGS engine_file_truncate_on_insert = 1;
CREATE TABLE $DB_PUBLIC.f AS file('$FILE_NAME', 'CSV');
CREATE USER $USER_POLICY;
GRANT CREATE ROW POLICY ON $DB_PUBLIC.tf TO $USER_POLICY;
GRANT CREATE ROW POLICY ON $DB_PUBLIC.f TO $USER_POLICY;
"

# attach creates the storage of the table function again, when it is read first
reset_tf()
{
    $CLICKHOUSE_CLIENT -m -q "DETACH TABLE $DB_PUBLIC.tf; ATTACH TABLE $DB_PUBLIC.tf;"
}

reset_tf
echo "policy on a table function which reads a hidden table"
$CLICKHOUSE_CLIENT --user "$USER_POLICY" -q "CREATE ROW POLICY p ON $DB_PUBLIC.tf USING k > 0 TO ALL" 2>&1 | grep -o -m1 -E "check the engine|UNKNOWN_TABLE"

# the answer must not depend on whether the hidden table exists
$CLICKHOUSE_CLIENT -q "DROP TABLE $DB_PRIVATE.src"
reset_tf
echo "policy on a table function which reads a hidden table which does not exist"
$CLICKHOUSE_CLIENT --user "$USER_POLICY" -q "CREATE ROW POLICY p ON $DB_PUBLIC.tf USING k > 0 TO ALL" 2>&1 | grep -o -m1 -E "check the engine|UNKNOWN_TABLE"

# a table function which reads a file does not read a table of the server
$CLICKHOUSE_CLIENT -m -q "DETACH TABLE $DB_PUBLIC.f; ATTACH TABLE $DB_PUBLIC.f;"
echo "policy on a table function which reads a file"
$CLICKHOUSE_CLIENT --user "$USER_POLICY" -q "CREATE ROW POLICY p ON $DB_PUBLIC.f USING c1 > 0 TO ALL" 2>&1 && echo "ok"

$CLICKHOUSE_CLIENT -m -q "
DROP ROW POLICY p ON $DB_PUBLIC.f;
DROP USER $USER_POLICY;
DROP DATABASE $DB_PUBLIC;
DROP DATABASE $DB_PRIVATE;
"
