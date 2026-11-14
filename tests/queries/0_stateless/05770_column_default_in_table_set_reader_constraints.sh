#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With parallel replicas a part may be read by a replica, which runs the query as the user of the
# cluster (the test cluster has no secret), so the set would not be built on behalf of the test user.
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --enable_parallel_replicas=0"

# A default `p IN table` of a column missing in a part is evaluated with the settings of the storage, except for the
# ones restricting the reading query. A setting that the profile of the reading user pins with a constraint also
# restricts what the user may read, so the settings of the storage must not lift it: here `allow_executable_tables`.

user="user_${CLICKHOUSE_DATABASE}"
profile="profile_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
    DROP USER IF EXISTS ${user};
    DROP SETTINGS PROFILE IF EXISTS ${profile};
    DROP TABLE IF EXISTS t_constraints;
    DROP TABLE IF EXISTS exec_rhs_constraints;

    -- The script does not exist: reading the table fails with \`UNSUPPORTED_METHOD\` when it is allowed.
    CREATE TABLE exec_rhs_constraints (p UInt8) ENGINE = Executable('nonexist.sh', 'TSV');

    CREATE TABLE t_constraints (p UInt8) ENGINE = MergeTree ORDER BY p;
    INSERT INTO t_constraints VALUES (1);
    ALTER TABLE t_constraints ADD COLUMN f UInt8 DEFAULT p IN exec_rhs_constraints;

    CREATE USER ${user} IDENTIFIED WITH no_password;
    CREATE SETTINGS PROFILE ${profile} SETTINGS allow_executable_tables = 0 CONST TO ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_constraints TO ${user};
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.exec_rhs_constraints TO ${user};
"

echo 'owner'
$CLICKHOUSE_CLIENT -q "SELECT sum(f) FROM t_constraints" 2>&1 | grep -o -m1 'UNSUPPORTED_METHOD'

echo 'user with allow_executable_tables = 0 CONST'
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT sum(p IN exec_rhs_constraints) FROM t_constraints" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT sum(f) FROM t_constraints" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'
# The column that has no default reading another table is still readable.
$CLICKHOUSE_CLIENT --user "${user}" -q "SELECT p FROM t_constraints"

$CLICKHOUSE_CLIENT -m -q "
    DROP SETTINGS PROFILE ${profile};
    DROP USER ${user};
    DROP TABLE t_constraints;
    DROP TABLE exec_rhs_constraints;
"
