#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A column added by `ALTER` with `DEFAULT p IN <table>` is physically missing from the parts written
# before, so reading it builds the set while the part is read. That has to happen on behalf of the
# reading user: with their privileges and row policies, not with the full access of the storage.

user="user_${CLICKHOUSE_DATABASE}"
policy="policy_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
    DROP USER IF EXISTS ${user};
    DROP ROW POLICY IF EXISTS ${policy} ON secret_keys;
    DROP ROW POLICY IF EXISTS ${policy}_others ON secret_keys;
    DROP TABLE IF EXISTS secret_keys;
    DROP TABLE IF EXISTS t_reader;
    DROP TABLE IF EXISTS t_slow;
    DROP VIEW IF EXISTS slow_keys;

    CREATE TABLE secret_keys (p UInt8) ENGINE = MergeTree ORDER BY p;
    INSERT INTO secret_keys VALUES (1), (2);

    CREATE TABLE t_reader (p UInt8) ENGINE = MergeTree ORDER BY p;
    INSERT INTO t_reader VALUES (1), (2), (3);
    ALTER TABLE t_reader ADD COLUMN f UInt8 DEFAULT p IN secret_keys;

    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_reader TO ${user};
"

echo 'owner'
$CLICKHOUSE_CLIENT -q "SELECT p, f FROM t_reader ORDER BY p"

echo 'no SELECT on the right-hand table'
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT p, f FROM t_reader ORDER BY p" 2>&1 | grep -o -m1 'ACCESS_DENIED'
# The column that has no default reading another table is still readable.
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT p FROM t_reader ORDER BY p"

echo 'row policy on the right-hand table'
$CLICKHOUSE_CLIENT -m -q "
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_keys TO ${user};
    CREATE ROW POLICY ${policy} ON secret_keys USING p = 2 TO ${user};
    -- Other users keep seeing every row even if the server is configured with
    -- \`users_without_row_policies_can_read_rows = false\`.
    CREATE ROW POLICY ${policy}_others ON secret_keys USING 1 TO ALL EXCEPT ${user};
"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT p, f FROM t_reader ORDER BY p"
echo 'owner is not affected by the policy'
$CLICKHOUSE_CLIENT -q "SELECT p, f FROM t_reader ORDER BY p"

# A merge has no reading query, so there is nobody to read the right-hand side on behalf of. It must not
# read it with the full access of the storage and store the result in the merged part, where the stored
# column would disclose the membership to a user with no access to the right-hand side.
echo 'merge does not materialize the default'
$CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE t_reader FINAL" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT p, f FROM t_reader ORDER BY p"
$CLICKHOUSE_CLIENT -q "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.secret_keys FROM ${user}"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT p, f FROM t_reader ORDER BY p" 2>&1 | grep -o -m1 'ACCESS_DENIED'

# The set is limited by the settings of the reading query.
echo 'set size limit of the read'
$CLICKHOUSE_CLIENT -q "SELECT p, f FROM t_reader ORDER BY p SETTINGS max_rows_in_set = 1, set_overflow_mode = 'throw'" 2>&1 | grep -o -m1 'SET_SIZE_LIMIT_EXCEEDED'

# The set is built under the query status of the read, so a limit of the read stops building it,
# instead of scanning the whole right-hand side. The right-hand side reads `system.numbers`, which
# has no estimate of the rows, and spends enough time on every row; the limits of the read on the
# number of rows are disabled, so that only the time limit can stop it.
echo 'limit of the read stops building the set'
$CLICKHOUSE_CLIENT -m -q "
    CREATE VIEW slow_keys AS SELECT toUInt8(number) AS p FROM system.numbers WHERE sipHash64(repeat(toString(number), 1000)) = 0;
    CREATE TABLE t_slow (p UInt8) ENGINE = MergeTree ORDER BY p;
    INSERT INTO t_slow VALUES (1);
    ALTER TABLE t_slow ADD COLUMN f UInt8 DEFAULT p IN slow_keys;
"
$CLICKHOUSE_CLIENT -q "SELECT f FROM t_slow SETTINGS max_execution_time = 1, max_rows_to_read = 0, max_bytes_to_read = 0" 2>&1 | grep -o -m1 'TIMEOUT_EXCEEDED'

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE t_slow;
    DROP VIEW slow_keys;
    DROP ROW POLICY ${policy} ON secret_keys;
    DROP ROW POLICY ${policy}_others ON secret_keys;
    DROP USER ${user};
    DROP TABLE t_reader;
    DROP TABLE secret_keys;
"
