#!/usr/bin/env bash
# Access checks of INSERT ... RETURNING: the destinations which read from other tables (including materialized views)
# are rejected only after the access check of the insert, and the inserted data of a table function
# does not require grants on a synthetic table name, while the tables read by the RETURNING query are still checked.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_05441_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} IDENTIFIED WITH no_password;
    GRANT CREATE TEMPORARY TABLE ON *.* TO ${user};

    CREATE TABLE t_src (x UInt64) ENGINE = Memory;
    CREATE TABLE t_dst (x UInt64) ENGINE = Memory;
    CREATE MATERIALIZED VIEW t_mv TO t_dst AS SELECT x FROM t_src;
    CREATE TABLE t_alias ENGINE = Alias('t_dst') SETTINGS allow_experimental_alias_table_engine = 1;
    CREATE TABLE t_secret (x UInt64) ENGINE = Memory;
"

echo "Materialized view"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_mv RETURNING (SELECT x FROM t_mv) SELECT 19" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_dst"

echo "No access to the destination"
${CLICKHOUSE_CLIENT} --user "${user}" --query "INSERT INTO ${CLICKHOUSE_DATABASE}.t_alias RETURNING (SELECT 1) SELECT 8" 2>&1 | grep -o -m1 "ACCESS_DENIED\|NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} --user "${user}" --query "INSERT INTO ${CLICKHOUSE_DATABASE}.t_mv RETURNING (SELECT 1) SELECT 8" 2>&1 | grep -o -m1 "ACCESS_DENIED\|NOT_IMPLEMENTED"

echo "Table function"
${CLICKHOUSE_CLIENT} --user "${user}" --query "INSERT INTO FUNCTION null('x UInt64') RETURNING (SELECT x) SELECT toUInt64(8)"
${CLICKHOUSE_CLIENT} --user "${user}" --query "INSERT INTO FUNCTION null('x UInt64') RETURNING (SELECT x * 2) SELECT toUInt64(8)"
${CLICKHOUSE_CLIENT} --user "${user}" --query "INSERT INTO FUNCTION null('x UInt64') RETURNING (SELECT count() FROM ${CLICKHOUSE_DATABASE}.t_secret) SELECT toUInt64(8)" 2>&1 | grep -o -m1 "ACCESS_DENIED"

${CLICKHOUSE_CLIENT} --query "
    DROP USER ${user};
    DROP TABLE t_alias;
    DROP TABLE t_mv;
    DROP TABLE t_src;
    DROP TABLE t_dst;
    DROP TABLE t_secret;
"
