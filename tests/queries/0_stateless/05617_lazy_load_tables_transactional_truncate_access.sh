#!/usr/bin/env bash
# Tags: no-replicated-database, no-ordinary-database, no-fasttest
# no-ordinary-database: transactions need an `Atomic` database for the default database of the test.
# no-fasttest: transactions are not enabled in the fast test.
# A transactional `TRUNCATE` asks the table whether it supports transactions, and for a table of a
# database with `lazy_load_tables` that loads the table and reveals its engine. A user who may not
# truncate the table must be rejected before that: with `ACCESS_DENIED`, not with a message naming
# the engine, and leaving the table unloaded.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_DATABASE}_lazy"
USER="user_${CLICKHOUSE_DATABASE}"

cleanup() {
    ${CLICKHOUSE_CLIENT} -q "DROP DATABASE IF EXISTS \`${DB}\` SYNC" 2>/dev/null
    ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS \`${USER}\`" 2>/dev/null
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} -m -q "
    CREATE DATABASE \`${DB}\` ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE \`${DB}\`.log (id UInt64) ENGINE = Log;
    INSERT INTO \`${DB}\`.log VALUES (1);
    CREATE TABLE \`${DB}\`.mt (id UInt64) ENGINE = MergeTree ORDER BY id;
    INSERT INTO \`${DB}\`.mt VALUES (1);
    DETACH DATABASE \`${DB}\`;
    ATTACH DATABASE \`${DB}\`;
    CREATE USER \`${USER}\` IDENTIFIED WITH no_password;
    GRANT SHOW TABLES ON \`${DB}\`.* TO \`${USER}\`;
"

for table in log mt
do
    ${CLICKHOUSE_CLIENT} --user "${USER}" -m -q "
        BEGIN TRANSACTION;
        TRUNCATE TABLE \`${DB}\`.${table};
    " 2>&1 | grep -o -m1 -e ACCESS_DENIED -e NOT_IMPLEMENTED
    ${CLICKHOUSE_CLIENT} -q "SELECT name, engine FROM system.tables WHERE database = '${DB}' AND name = '${table}'"
done
