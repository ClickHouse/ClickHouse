#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Both inserts must land in the same async insert batch, so the queue is flushed only by SYSTEM FLUSH ASYNC INSERT QUEUE.
url="${CLICKHOUSE_URL}&async_insert=1&wait_for_async_insert=0&async_insert_use_adaptive_busy_timeout=0&async_insert_busy_timeout_min_ms=600000&async_insert_busy_timeout_max_ms=600000"

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_async_json_each_row"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_async_json_broken_meta"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_async_json_each_row (id UInt64, s String) ENGINE = MergeTree ORDER BY id"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_async_json_broken_meta (id UInt64, s String) ENGINE = MergeTree ORDER BY id"

document='{"meta": [{"name": "id", "type": "UInt64"}, {"name": "s", "type": "String"}], "data": [{"id": 2, "s": "b"}], "rows": 1}'

function insert_pair()
{
    local table=$1 first=$2
    ${CLICKHOUSE_CURL} -sS "${url}&query=INSERT+INTO+${table}+FORMAT+JSON" --data-binary "$first"
    ${CLICKHOUSE_CURL} -sS "${url}&query=INSERT+INTO+${table}+FORMAT+JSON" --data-binary "$document"
    ${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH ASYNC INSERT QUEUE ${table}"
    ${CLICKHOUSE_CLIENT} -q "SELECT * FROM ${table} ORDER BY id"
}

# The first entry has no metadata or broken metadata, so it is parsed as JSONEachRow.
insert_pair t_async_json_each_row '{"id": 1, "s": "a"}'
insert_pair t_async_json_broken_meta '{"meta": [{"name": "id"'

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_async_json_each_row"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_async_json_broken_meta"
