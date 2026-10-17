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

echo "Data from stdin is not supported"
echo '14' | ${CLICKHOUSE_CLIENT} --query "INSERT INTO t_returning_client (x) RETURNING (SELECT x FROM t_returning_client) FORMAT TSV" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'

echo "clickhouse-local"
${CLICKHOUSE_LOCAL} --query "
    CREATE TABLE t (x UInt64, y UInt64 DEFAULT x * 2) ENGINE = Memory;
    INSERT INTO t (x) RETURNING (SELECT x, y FROM t ORDER BY x) VALUES (1), (2);
    INSERT INTO t (x) RETURNING (SELECT count() FROM t) SELECT number FROM numbers(10);
    SELECT count() FROM t;
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_returning_client"
