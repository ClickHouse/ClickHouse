#!/usr/bin/env bash

# `rows_before_limit_at_least` and `rows_before_aggregation` of a query on an `ABR` table are those of the same query on its
# source table: the attempt computes them, and they are reported with the result it replays, also when it is empty, and also when a
# limit above the `ABR` table stops the replay early, as it does with `exact_rows_before_limit = 0`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_abr 1 -q "
CREATE TABLE src_1 (ts DateTime, value UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY ts;
INSERT INTO src_1 SELECT toDateTime('2026-01-01 00:00:00', 'UTC') + number, number FROM numbers(100);
CREATE TABLE abr ENGINE = ABR('src', [1], ('ts'));
"

# The source table is read as one block, so that a limit that stops early has still received all its rows.
settings="optimize_read_in_order = 0, rows_before_aggregation = 1, enable_parallel_replicas = 0, max_threads = 1, max_block_size = 65536"
bound="ts >= toDateTime('2026-01-01 00:00:00', 'UTC')"
empty="ts >= toDateTime('2027-01-01 00:00:00', 'UTC')"

# Prints the number of rows of the result of the query $1 with the extra settings $2, and `rows_before_limit_at_least` and `rows_before_aggregation` if they are reported.
statistics()
{
    ${CLICKHOUSE_CLIENT} -q "$1 SETTINGS ${settings}, exact_rows_before_limit = ${exact}${2:+, $2} FORMAT JSON" \
        | grep -oE '"(rows|rows_before_limit_at_least|rows_before_aggregation)": [0-9]+' | tr '\n' ' '
    echo
}

for exact in 0 1; do
    for table in src_1 abr; do
        echo "-- ${table}, exact_rows_before_limit = ${exact}"
        statistics "SELECT value FROM ${table} WHERE ${bound} ORDER BY value LIMIT 1"
        statistics "SELECT value FROM ${table} WHERE ${empty} ORDER BY value LIMIT 1"
        statistics "SELECT * FROM (SELECT value FROM ${table} WHERE ${bound}) LIMIT 1"
        statistics "SELECT value FROM (SELECT DISTINCT value FROM ${table} WHERE ${bound}) LIMIT 1" "query_plan_push_down_limit = 0"
        statistics "SELECT sum(value) FROM ${table} WHERE ${bound}"
        statistics "SELECT value FROM ${table} WHERE ${bound} ORDER BY value"
    done
done

${CLICKHOUSE_CLIENT} -q "DROP TABLE abr; DROP TABLE src_1"
