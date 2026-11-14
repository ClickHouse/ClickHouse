#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `CHECK` constraint may use a `Set` table on the right-hand side of `IN`. Whether the table exists and which
# engine it has decides whether the constraint is accepted, so the outcome must not reveal a table the user
# cannot see: without access to the table, `CREATE TABLE` and `ALTER TABLE ... ADD CONSTRAINT` fail with
# `ACCESS_DENIED`, whether the table is a `Set` table, a `MergeTree` table, or does not exist.

SECRET_DB="${CLICKHOUSE_DATABASE}_secret"
USER="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -q "
    DROP DATABASE IF EXISTS ${SECRET_DB};
    DROP USER IF EXISTS ${USER};
    CREATE DATABASE ${SECRET_DB};
    CREATE TABLE ${SECRET_DB}.set_target (x UInt64) ENGINE = Set;
    CREATE TABLE ${SECRET_DB}.mt_target (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_alter (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE USER ${USER};
    GRANT CREATE TABLE, ALTER ADD CONSTRAINT ON ${CLICKHOUSE_DATABASE}.* TO ${USER};
    GRANT TABLE ENGINE ON MergeTree TO ${USER};
"

for target in set_target mt_target absent_target
do
    echo "--- ${target}"
    $CLICKHOUSE_CLIENT --user "${USER}" -q "
        CREATE TABLE ${CLICKHOUSE_DATABASE}.t_create (x UInt64, CONSTRAINT c CHECK x IN ${SECRET_DB}.${target})
        ENGINE = MergeTree ORDER BY x" 2>&1 | grep -o -m1 'ACCESS_DENIED\|BAD_ARGUMENTS'
    $CLICKHOUSE_CLIENT --user "${USER}" -q "
        ALTER TABLE ${CLICKHOUSE_DATABASE}.t_alter ADD CONSTRAINT c CHECK x IN ${SECRET_DB}.${target}" 2>&1 | grep -o -m1 'ACCESS_DENIED\|BAD_ARGUMENTS'
done

echo "--- granted"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${SECRET_DB}.* TO ${USER}"
$CLICKHOUSE_CLIENT --user "${USER}" -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_create (x UInt64, CONSTRAINT c CHECK x IN ${SECRET_DB}.set_target)
    ENGINE = MergeTree ORDER BY x" && echo "set_target: created"
$CLICKHOUSE_CLIENT --user "${USER}" -q "
    CREATE TABLE ${CLICKHOUSE_DATABASE}.t_create_mt (x UInt64, CONSTRAINT c CHECK x IN ${SECRET_DB}.mt_target)
    ENGINE = MergeTree ORDER BY x" 2>&1 | grep -o -m1 'ACCESS_DENIED\|BAD_ARGUMENTS'

$CLICKHOUSE_CLIENT -q "
    DROP USER ${USER};
    DROP TABLE ${CLICKHOUSE_DATABASE}.t_create;
    DROP TABLE ${CLICKHOUSE_DATABASE}.t_alter;
    DROP DATABASE ${SECRET_DB};
"
