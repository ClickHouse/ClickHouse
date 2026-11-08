#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_returning_client;
    CREATE TABLE t_returning_client (id UUID DEFAULT generateUUIDv4(), x UInt64) ENGINE = MergeTree ORDER BY x;
"

echo "The returned generated values are the same as the inserted ones"
returned=$(${CLICKHOUSE_CLIENT} --query "INSERT INTO t_returning_client (x) RETURNING (SELECT id, x FROM t_returning_client ORDER BY x) VALUES (1), (2), (3)")
stored=$(${CLICKHOUSE_CLIENT} --query "SELECT id, x FROM t_returning_client ORDER BY x")
[ "$returned" == "$stored" ] && echo "OK" || echo "FAIL: '$returned' != '$stored'"

echo "HTTP with the data in the request body"
echo -e '10\n11' | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT+INTO+t_returning_client+(x)+RETURNING+(SELECT+count(),+sum(x)+FROM+t_returning_client)+FORMAT+TSV" --data-binary @-
${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&default_format=JSONEachRow" -d "INSERT INTO t_returning_client (x) RETURNING (SELECT x FROM t_returning_client ORDER BY x) VALUES (12), (13)"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_returning_client"

echo "Data from stdin"
echo -e '14\n15' | ${CLICKHOUSE_CLIENT} --query "INSERT INTO t_returning_client (x) RETURNING (SELECT x FROM t_returning_client ORDER BY x) FORMAT TSV"
echo -e '16\n17' | ${CLICKHOUSE_CLIENT} --query "INSERT INTO t_returning_client (x) RETURNING (SELECT x * 10 FROM t_returning_client ORDER BY x) SELECT x + 1 FROM input('x UInt64') FORMAT TSV"

echo "FROM INFILE"
echo -e '20\n21' > "${CLICKHOUSE_TMP}/05402_data.tsv"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_returning_client (x) RETURNING (SELECT sum(x)) FROM INFILE '${CLICKHOUSE_TMP}/05402_data.tsv' FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_returning_client"

echo "A large result is received while the data is sent"
seq 1 1000000 | ${CLICKHOUSE_CLIENT} --max_insert_block_size 100000 --query "INSERT INTO FUNCTION null('x UInt64') RETURNING (SELECT x) FORMAT TSV" | awk '{ s += $1 } END { print NR, s }'

echo "clickhouse-local"
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (x UInt64, y UInt64 DEFAULT x * 2) ENGINE = Memory;
    INSERT INTO t (x) RETURNING (SELECT x, y FROM t ORDER BY x) VALUES (1), (2);
    INSERT INTO t (x) RETURNING (SELECT count() FROM t) SELECT number FROM numbers(10);
    SELECT count() FROM t;
"
echo -e '1\n2\n3' | ${CLICKHOUSE_LOCAL} --query "INSERT INTO FUNCTION null('x UInt64') RETURNING (SELECT sum(x)) FORMAT TSV"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_returning_client"
