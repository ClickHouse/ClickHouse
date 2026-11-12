#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database, no-fasttest
# no-ordinary-database: transactions need an `Atomic` database.
# no-fasttest: transactions are not enabled in the fast test.
# `CREATE OR REPLACE TABLE` drops the replaced table (or, on failure, its temporary table) under an internal
# random name with `no_access_check`. Inside a transaction, that `DROP` must not be authorized against the
# random name: a user granted only on the real table name must be able to replace it and to have a failed
# replacement cleaned up.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_replace"
USER="user_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`${DB}\` SYNC" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS \`${USER}\`" 2>/dev/null
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -m -q "
    CREATE DATABASE \`${DB}\` ENGINE = Atomic;
    CREATE TABLE \`${DB}\`.t (n UInt64) ENGINE = Memory;
    INSERT INTO \`${DB}\`.t VALUES (1);
    CREATE USER \`${USER}\` IDENTIFIED WITH no_password;
    GRANT CREATE TABLE, DROP TABLE, INSERT ON \`${DB}\`.t TO \`${USER}\`;
    GRANT TABLE ENGINE ON Memory TO \`${USER}\`;
"

SETTINGS="--implicit_transaction 1 --throw_on_unsupported_query_inside_transaction 0"

# A successful replacement drops the replaced table.
${CLICKHOUSE_CLIENT} --user "${USER}" ${SETTINGS} -q "CREATE OR REPLACE TABLE \`${DB}\`.t (s String) ENGINE = Memory"
${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.columns WHERE database = '${DB}' AND table = 't'"

# A failed replacement drops its temporary table.
${CLICKHOUSE_CLIENT} --user "${USER}" ${SETTINGS} -q "CREATE OR REPLACE TABLE \`${DB}\`.t ENGINE = Memory AS SELECT throwIf(1) AS x" 2>&1 | grep -o -m1 FUNCTION_THROW_IF_VALUE_IS_NON_ZERO

${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = '${DB}' ORDER BY name"
