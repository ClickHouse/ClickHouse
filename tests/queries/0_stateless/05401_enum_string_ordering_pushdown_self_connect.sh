#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the PostgreSQL and MySQL table engines, which the fast test does not build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# an ordering of an Enum column and a string compares by the Enum value, so it stays local
# the remote database would compare the stored strings, and here the remote database is this very server

USER_NAME="pg_my_enum_order_${CLICKHOUSE_DATABASE}"
PG_HOST="localhost:${CLICKHOUSE_PORT_POSTGRESQL}"
MY_HOST="127.0.0.1:${CLICKHOUSE_PORT_MYSQL}"

echo "
DROP USER IF EXISTS ${USER_NAME};
CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY 'pgpass';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};

CREATE TABLE src (k UInt32, e String, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO src VALUES (1, 'z', 'm'), (2, 'a', 'z'), (3, 'm', 'a');

CREATE TABLE pg_t (k UInt32, e Enum8('z' = 1, 'a' = 2, 'm' = 3), s String)
ENGINE = PostgreSQL('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'src', '${USER_NAME}', 'pgpass');
CREATE TABLE my_t (k UInt32, e Enum8('z' = 1, 'a' = 2, 'm' = 3), s String)
ENGINE = MySQL('${MY_HOST}', '${CLICKHOUSE_DATABASE}', 'src', '${USER_NAME}', 'pgpass');
" | $CLICKHOUSE_CLIENT

for t in pg_t my_t
do
    echo "--- $t"
    $CLICKHOUSE_CLIENT --query "SELECT 'e < s', groupArray(e) FROM (SELECT e FROM $t WHERE e < s ORDER BY k)"
    $CLICKHOUSE_CLIENT --query "SELECT 'e < m', groupArray(e) FROM (SELECT e FROM $t WHERE e < 'm' ORDER BY k)"
    $CLICKHOUSE_CLIENT --query "SELECT '(e, k) > (a, 0)', groupArray(e) FROM (SELECT e FROM $t WHERE (e, k) > ('a', 0) ORDER BY k)"
    $CLICKHOUSE_CLIENT --query "SELECT 'toString(e) < s', groupArray(e) FROM (SELECT e FROM $t WHERE toString(e) < s ORDER BY k)"
    $CLICKHOUSE_CLIENT --query "SELECT 'e = m', groupArray(e) FROM (SELECT e FROM $t WHERE e = 'm' ORDER BY k)"
    $CLICKHOUSE_CLIENT --query "SELECT 'e = m AND e > z', groupArray(e) FROM (SELECT e FROM $t WHERE e = 'm' AND e > 'z' ORDER BY k)"
done

$CLICKHOUSE_CLIENT --query "DROP USER ${USER_NAME}"
