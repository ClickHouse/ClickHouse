#!/usr/bin/env bash
# The RETURNING query of INSERT reads the inserted data under the name of the destination table,
# so it requires the SELECT privilege for this table, and the row policies of the table filter the inserted rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user_insert="u_insert_${CLICKHOUSE_DATABASE}"
user_policy="u_policy_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${user_insert}, ${user_policy};
    CREATE USER ${user_insert} IDENTIFIED WITH no_password;
    CREATE USER ${user_policy} IDENTIFIED WITH no_password;

    CREATE TABLE t_returning_access (id UInt64, tenant String, code UInt64 DEFAULT id * 10) ENGINE = MergeTree ORDER BY id;
    GRANT INSERT ON t_returning_access TO ${user_insert};
    GRANT INSERT, SELECT ON t_returning_access TO ${user_policy};
"

echo "INSERT without SELECT for the destination table"
${CLICKHOUSE_CLIENT} --user "${user_insert}" --query "INSERT INTO t_returning_access (id, tenant) RETURNING (SELECT id, code) VALUES (1, 'a')" 2>&1 | grep -o -m1 "necessary to have the grant SELECT(id, code)"
${CLICKHOUSE_CLIENT} --user "${user_insert}" --query "INSERT INTO t_returning_access (id, tenant) RETURNING (SELECT count()) VALUES (1, 'a')" 2>&1 | grep -o -m1 "necessary to have the grant SELECT for at least one column"
# The rejected queries insert nothing.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_returning_access"

echo "Row policy of the destination table"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY p_returning_access ON t_returning_access USING tenant = 'a' TO ${user_policy}"
${CLICKHOUSE_CLIENT} --user "${user_policy}" --query "INSERT INTO t_returning_access (id, tenant) RETURNING (SELECT id, tenant, code ORDER BY id) VALUES (2, 'a'), (3, 'b')"
${CLICKHOUSE_CLIENT} --user "${user_policy}" --query "INSERT INTO t_returning_access (id, tenant) RETURNING (SELECT count() FROM t_returning_access) SELECT number + 10, if(number % 2 = 0, 'a', 'b') FROM numbers(4)"
# All rows are inserted, including the rows which the row policy hides from the user.
${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY p_returning_access ON t_returning_access"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_returning_access"

${CLICKHOUSE_CLIENT} --query "
    DROP USER ${user_insert}, ${user_policy};
    DROP TABLE t_returning_access;
"
