#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A part written before `MODIFY COLUMN f UInt8` still stores `f` as `Nullable(UInt8)` until the mutation
# rewrites it, so reading it converts `f` while the part is read and substitutes the default of `f` for
# `NULL`. If that default reads a table (`DEFAULT 1 IN <table>`), the set has to be built on behalf of the
# reading user, as for a column missing in the part: with their privileges and row policies.

user="user_${CLICKHOUSE_DATABASE}"
policy="policy_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
    DROP USER IF EXISTS ${user};
    DROP TABLE IF EXISTS secret_keys;
    DROP TABLE IF EXISTS t_conversion;

    CREATE TABLE secret_keys (p UInt8) ENGINE = MergeTree ORDER BY p;
    INSERT INTO secret_keys VALUES (1), (2);

    CREATE TABLE t_conversion (p UInt8, f Nullable(UInt8)) ENGINE = MergeTree ORDER BY p;
    INSERT INTO t_conversion VALUES (1, NULL), (2, NULL), (3, NULL), (4, 5);

    -- Keep the part with the nullable column: the mutation of the type is never executed.
    SYSTEM STOP MERGES t_conversion;
    ALTER TABLE t_conversion MODIFY COLUMN f UInt8 DEFAULT 0 SETTINGS alter_sync = 0;
    -- Changing only the default needs no conversion of the data.
    ALTER TABLE t_conversion MODIFY COLUMN f UInt8 DEFAULT toUInt8(1) IN secret_keys SETTINGS alter_sync = 0;

    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_conversion TO ${user};
"

echo 'owner'
$CLICKHOUSE_CLIENT -q "SELECT p, f FROM t_conversion ORDER BY p SETTINGS apply_mutations_on_fly = 0"

echo 'no SELECT on the right-hand table'
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT p, f FROM t_conversion ORDER BY p SETTINGS apply_mutations_on_fly = 0" 2>&1 | grep -o -m1 'ACCESS_DENIED'

echo 'row policy on the right-hand table'
$CLICKHOUSE_CLIENT -m -q "
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_keys TO ${user};
    CREATE ROW POLICY ${policy} ON secret_keys USING p = 2 TO ${user};
    -- Other users keep seeing every row even if the server is configured with
    -- \`users_without_row_policies_can_read_rows = false\`.
    CREATE ROW POLICY ${policy}_others ON secret_keys USING 1 TO ALL EXCEPT ${user};
"
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT p, f FROM t_conversion ORDER BY p SETTINGS apply_mutations_on_fly = 0"

$CLICKHOUSE_CLIENT -m -q "
    DROP ROW POLICY ${policy} ON secret_keys;
    DROP ROW POLICY ${policy}_others ON secret_keys;
    DROP USER ${user};
    DROP TABLE t_conversion;
    DROP TABLE secret_keys;
"
