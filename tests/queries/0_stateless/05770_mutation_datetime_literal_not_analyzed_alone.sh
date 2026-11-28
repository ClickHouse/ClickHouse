#!/usr/bin/env bash
# With `session_timezone`, the expressions of a mutation and of a `TTL` are analyzed to convert their `DateTime` literals. An
# expression that cannot be analyzed alone does not fail the statement there, so it is accepted or rejected as without the
# session time zone: the value of an `UPDATE ... WHERE NULL`, which the mutation analyzes as a branch of `if` that is never
# resolved, is accepted, and an invalid `TTL` is rejected by its own validation, with its own error.
# https://github.com/ClickHouse/ClickHouse/issues/123959

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --session_timezone Asia/Tokyo --mutations_sync 2"

$CLIENT --query "
    DROP TABLE IF EXISTS t_not_analyzed_alone;
    CREATE TABLE t_not_analyzed_alone (id UInt64, v UInt64, event DateTime) ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_not_analyzed_alone VALUES (1, 20, 1577804400);"

echo "--- UPDATE ... WHERE NULL with a value that does not resolve alone"
$CLIENT --query "ALTER TABLE t_not_analyzed_alone UPDATE v = now(exists((SELECT 1))) WHERE NULL"
$CLIENT --query "SELECT id, v FROM t_not_analyzed_alone"

echo "--- an invalid TTL is rejected by its validation"
$CLIENT --query "ALTER TABLE t_not_analyzed_alone MODIFY TTL missing_column + INTERVAL 1 DAY" 2>&1 \
    | grep -o -m1 "Missing columns: 'missing_column'"
$CLIENT --query "
    CREATE TABLE t_not_analyzed_alone_ttl (id UInt64, event DateTime) ENGINE = MergeTree ORDER BY id
    TTL missing_column + INTERVAL 1 DAY" 2>&1 \
    | grep -o -m1 "Missing columns: 'missing_column'"

$CLIENT --query "DROP TABLE t_not_analyzed_alone"
