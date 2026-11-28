#!/usr/bin/env bash
# Tags: long, no-parallel-replicas
# ntile over parallel replicas fails, see https://github.com/ClickHouse/ClickHouse/issues/112401

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./window_functions_block_sizes.lib
. "$CUR_DIR"/window_functions_block_sizes.lib

create_table

{
check 'partition readers with an aggregate' "SELECT p, id, (ntile(3) OVER w, round(percent_rank() OVER w, 6), round(cume_dist() OVER w, 6), lag(v, 3) OVER w, lead(v, 3) OVER w, sum(v) OVER w) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id)"
check 'partition readers over a sliding frame' "SELECT p, id, (ntile(3) OVER w, lag(v, 3) OVER w, lead(v, 3) OVER w, lagInFrame(v, 2, -1) OVER w, leadInFrame(v, 2, -1) OVER w, sum(v) OVER w) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)"
} | $CLICKHOUSE_CLIENT
