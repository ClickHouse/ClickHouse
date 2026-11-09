#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `File` table whose records omit a column with `DEFAULT p IN <table>` builds the set while the format is read.
# That has to happen on behalf of the reading user, with their privileges and row policies, not with the access
# of the storage.

user="user_${CLICKHOUSE_DATABASE}"
policy="policy_${CLICKHOUSE_DATABASE}"
file_name="${CLICKHOUSE_DATABASE}_05570.jsonl"

$CLICKHOUSE_CLIENT -m -q "
    DROP USER IF EXISTS ${user};
    DROP ROW POLICY IF EXISTS ${policy} ON secret_keys;
    DROP ROW POLICY IF EXISTS ${policy}_others ON secret_keys;
    DROP TABLE IF EXISTS secret_keys;
    DROP TABLE IF EXISTS t_file_reader;

    CREATE TABLE secret_keys (p UInt8) ENGINE = MergeTree ORDER BY p;
    INSERT INTO secret_keys VALUES (1), (2);

    INSERT INTO FUNCTION file('${file_name}', 'JSONEachRow', 'p UInt8')
        SETTINGS engine_file_truncate_on_insert = 1 VALUES (1), (2), (3);

    CREATE TABLE t_file_reader (p UInt8, f UInt8 DEFAULT p IN secret_keys) ENGINE = File(JSONEachRow, '${file_name}');

    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_file_reader TO ${user};
"

echo 'owner'
$CLICKHOUSE_CLIENT -q "SELECT p, f FROM t_file_reader ORDER BY p"

echo 'no SELECT on the right-hand table'
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT p, f FROM t_file_reader ORDER BY p" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo 'row policy on the right-hand table'
$CLICKHOUSE_CLIENT -m -q "
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_keys TO ${user};
    CREATE ROW POLICY ${policy} ON secret_keys USING p = 2 TO ${user};
    -- Other users keep seeing every row even if the server is configured with
    -- \`users_without_row_policies_can_read_rows = false\`.
    CREATE ROW POLICY ${policy}_others ON secret_keys USING 1 TO ALL EXCEPT ${user};
"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT p, f FROM t_file_reader ORDER BY p"

$CLICKHOUSE_CLIENT -m -q "
    DROP USER ${user};
    DROP ROW POLICY ${policy} ON secret_keys;
    DROP ROW POLICY ${policy}_others ON secret_keys;
    DROP TABLE t_file_reader;
    DROP TABLE secret_keys;
"
