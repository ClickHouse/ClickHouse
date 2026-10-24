#!/usr/bin/env bash
# Tags: long, no-parallel-replicas
# ntile over parallel replicas fails, see https://github.com/ClickHouse/ClickHouse/issues/112401

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05293_window_functions_block_boundaries.lib
. "$CUR_DIR"/05293_window_functions_block_boundaries.lib

create_table

{
check 'ranking' "SELECT p, id, (row_number() OVER o, rank() OVER w, dense_rank() OVER w) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k), o AS (PARTITION BY p ORDER BY k, id)"
check 'partition readers' "SELECT p, id, (ntile(3) OVER n, round(percent_rank() OVER w, 6), round(cume_dist() OVER w, 6)) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k), n AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
check 'lag and lead beyond the block' "SELECT p, id, (lag(v, 5) OVER w, lead(v, 7) OVER w, lag(v, 1, -1) OVER w, lead(v) OVER w) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id)"
check 'offsets beyond the frame' "SELECT p, id, (lagInFrame(v, 3, -1) OVER f, leadInFrame(v, 3, -1) OVER f, lagInFrame(v, 2) OVER f, leadInFrame(v, 2) OVER f, nth_value(v, 4) OVER f, nth_value(v, 1) OVER f) AS r FROM t WINDOW f AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING)"
check 'decayed over a sliding frame' "SELECT p, id, (round(exponentialTimeDecayedSum(5)(v, id) OVER d, 6), round(exponentialTimeDecayedMax(5)(v, id) OVER d, 6), round(exponentialTimeDecayedCount(5)(id) OVER d, 6), round(exponentialTimeDecayedAvg(5)(v, id) OVER d, 6)) AS r FROM t WINDOW d AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING)"
check 'decayed over a growing frame' "SELECT p, id, (round(exponentialTimeDecayedSum(5)(v, id) OVER d, 6), round(exponentialTimeDecayedCount(5)(id) OVER d, 6), round(exponentialTimeDecayedAvg(5)(v, id) OVER d, 6)) AS r FROM t WINDOW d AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
check 'general with aggregates' "SELECT p, id, (rank() OVER w, lagInFrame(v) OVER w, sum(v) OVER w, count() OVER w) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
} | $CLICKHOUSE_CLIENT
