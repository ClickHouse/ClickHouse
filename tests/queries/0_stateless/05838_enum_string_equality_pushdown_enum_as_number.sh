#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs the PostgreSQL and MySQL table engines, which the fast test does not build

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# under input_format_tsv_enum_as_number, = and != read a string constant as the Enum number, so they stay local
# the remote database would compare the stored names, and here the remote database is this very server

USER_NAME="pg_my_enum_eq_${CLICKHOUSE_DATABASE}"
PG_HOST="localhost:${CLICKHOUSE_PORT_POSTGRESQL}"
MY_HOST="127.0.0.1:${CLICKHOUSE_PORT_MYSQL}"

echo "
DROP USER IF EXISTS ${USER_NAME};
CREATE USER ${USER_NAME} IDENTIFIED WITH plaintext_password BY 'pgpass';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${USER_NAME};

CREATE TABLE src (k UInt32, e String) ENGINE = MergeTree ORDER BY k;
INSERT INTO src VALUES (1, 'z'), (2, 'a'), (3, '1'), (4, 'b');

CREATE TABLE local_t (k UInt32, e Enum8('z' = 1, 'a' = 2, '1' = 3, 'b' = 4)) ENGINE = Memory;
INSERT INTO local_t SELECT k, e FROM src;
CREATE TABLE pg_t (k UInt32, e Enum8('z' = 1, 'a' = 2, '1' = 3, 'b' = 4))
ENGINE = PostgreSQL('${PG_HOST}', '${CLICKHOUSE_DATABASE}', 'src', '${USER_NAME}', 'pgpass');
CREATE TABLE my_t (k UInt32, e Enum8('z' = 1, 'a' = 2, '1' = 3, 'b' = 4))
ENGINE = MySQL('${MY_HOST}', '${CLICKHOUSE_DATABASE}', 'src', '${USER_NAME}', 'pgpass');
" | $CLICKHOUSE_CLIENT

SETTINGS="input_format_tsv_enum_as_number = 1, optimize_redundant_comparisons = 0, optimize_and_compare_chain = 0"

for t in local_t pg_t my_t
do
    echo "--- $t"
    for condition in "e = '1'" "e != '1'" "'1' = e" "e = '1' AND e < '1'" "(e, k) = ('1', 1)" "e IN ('1')"
    do
        echo -n "$condition: "
        $CLICKHOUSE_CLIENT --query "SELECT groupArray(e) FROM (SELECT e FROM $t WHERE $condition ORDER BY k) SETTINGS $SETTINGS"
    done
done

$CLICKHOUSE_CLIENT --query "DROP USER ${USER_NAME}"
