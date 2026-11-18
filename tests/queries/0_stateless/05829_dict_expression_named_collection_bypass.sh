#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: named collections are server-global, and `DETACH TABLE` is not allowed in a `Replicated` database.

# An attribute `EXPRESSION` (and the source `WHERE` / `QUERY`) is raw SQL executed on the dictionary source.
# When the source resolves a named collection that confines the data with a `NOT OVERRIDABLE` `table` / `database`,
# such raw SQL must not be allowed to read outside the confined scope, otherwise it bypasses the lock.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

nc_locked="${CLICKHOUSE_TEST_UNIQUE_NAME}_locked"
nc_open="${CLICKHOUSE_TEST_UNIQUE_NAME}_open"
user="${CLICKHOUSE_TEST_UNIQUE_NAME}_user"

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --multiquery --query "
        SET ignore_drop_queries_probability = 0;
        DROP DICTIONARY IF EXISTS dict_base;
        DROP DICTIONARY IF EXISTS dict_leak;
        DROP DICTIONARY IF EXISTS dict_leak_where;
        DROP DICTIONARY IF EXISTS dict_open_expr;
        DROP TABLE IF EXISTS safe_table;
        DROP TABLE IF EXISTS secret_table;
        DROP USER IF EXISTS $user;
        DROP NAMED COLLECTION IF EXISTS $nc_locked;
        DROP NAMED COLLECTION IF EXISTS $nc_open;
    "
}
trap cleanup EXIT
cleanup

${CLICKHOUSE_CLIENT} --multiquery --query "
    CREATE TABLE safe_table (id UInt64, value UInt64) ENGINE = Memory;
    CREATE TABLE secret_table (id UInt64, value UInt64) ENGINE = Memory;
    INSERT INTO safe_table VALUES (1, 100);
    INSERT INTO secret_table VALUES (1, 999);

    CREATE NAMED COLLECTION $nc_locked AS
        host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP}, user = 'default', password = '',
        db = '${CLICKHOUSE_DATABASE}', table = 'safe_table' NOT OVERRIDABLE;
    CREATE NAMED COLLECTION $nc_open AS
        host = '127.0.0.1', port = ${CLICKHOUSE_PORT_TCP}, user = 'default', password = '',
        db = '${CLICKHOUSE_DATABASE}', table = 'safe_table';

    CREATE USER $user;
    GRANT SOURCES ON *.* TO $user;
    GRANT CREATE DICTIONARY, DROP DICTIONARY, dictGet ON ${CLICKHOUSE_DATABASE}.* TO $user;
    GRANT NAMED COLLECTION ON $nc_locked TO $user;
    GRANT NAMED COLLECTION ON $nc_open TO $user;
"

echo 'A confined dictionary reads only the locked table'
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    CREATE DICTIONARY dict_base (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(NAME $nc_locked)) LAYOUT(FLAT()) LIFETIME(0);
    SELECT dictGet('dict_base', 'value', toUInt64(1));
"

secret="(SELECT value FROM ${CLICKHOUSE_DATABASE}.secret_table LIMIT 1)"

echo 'An attribute EXPRESSION that reads another table is rejected under the lock'
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    CREATE DICTIONARY dict_leak
        (id UInt64, value UInt64 EXPRESSION '$secret')
    PRIMARY KEY id SOURCE(CLICKHOUSE(NAME $nc_locked)) LAYOUT(FLAT()) LIFETIME(0); -- { serverError BAD_ARGUMENTS }
"

echo 'A source WHERE subquery is rejected under the lock'
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    CREATE DICTIONARY dict_leak_where (id UInt64, value UInt64) PRIMARY KEY id
    SOURCE(CLICKHOUSE(NAME $nc_locked WHERE 'id = $secret')) LAYOUT(FLAT()) LIFETIME(0); -- { serverError BAD_ARGUMENTS }
"

echo 'The same EXPRESSION is allowed when the collection does not lock the confining keys'
${CLICKHOUSE_CLIENT} --user "$user" --multiquery --query "
    CREATE DICTIONARY dict_open_expr
        (id UInt64, value UInt64 EXPRESSION '$secret')
    PRIMARY KEY id SOURCE(CLICKHOUSE(NAME $nc_open)) LAYOUT(FLAT()) LIFETIME(0);
    SELECT dictGet('dict_open_expr', 'value', toUInt64(1));
"
