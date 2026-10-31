#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A secondary query sent by a client has an empty initial query id. The self-reading INSERT into a `Join` table
# must still detect that it already holds the read lock, instead of waiting for the lock acquire timeout.

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS test_table_join;
    CREATE TABLE test_table_join (id UInt64, value String) ENGINE = Join(Any, Left, id);
    INSERT INTO test_table_join VALUES (1, 'q');
"

for query in \
    "INSERT INTO test_table_join SELECT * FROM test_table_join" \
    "INSERT INTO test_table_join SELECT * FROM (SELECT 1 AS id) AS t1 ANY LEFT JOIN test_table_join USING (id)" \
    "INSERT INTO test_table_join SELECT id, toString(id) FROM (SELECT 1 AS id) AS t1 ANY LEFT JOIN (SELECT id FROM test_table_join) AS t2 USING (id)"
do
    for analyzer in 0 1
    do
        $CLICKHOUSE_CLIENT --query_kind secondary_query --enable_analyzer "$analyzer" --lock_acquire_timeout 60 -q "$query" 2>&1 | grep -o -m1 -e DEADLOCK_AVOIDED -e TIMEOUT_EXCEEDED || echo "no error"
    done
done

$CLICKHOUSE_CLIENT -q "SELECT * FROM test_table_join; DROP TABLE test_table_join;"
