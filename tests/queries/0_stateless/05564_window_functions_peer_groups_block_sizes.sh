#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./05293_window_functions_block_boundaries.lib
. "$CUR_DIR"/05293_window_functions_block_boundaries.lib

create_table

{
check 'ranking with an aggregate under the default frame' "SELECT p, id, (rank() OVER w, dense_rank() OVER w, sum(v) OVER w) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k)"
check 'ranking with an aggregate under a range offset frame' "SELECT p, id, (rank() OVER w, dense_rank() OVER w, sum(v) OVER w, round(cume_dist() OVER c, 6)) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING), c AS (PARTITION BY p ORDER BY k)"
check 'ranking with an aggregate under a groups frame' "SELECT p, id, (rank() OVER w, dense_rank() OVER w, sum(v) OVER w, count() OVER w) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
check 'groups frame without ties' "SELECT p, id, (dense_rank() OVER w, sum(v) OVER w) AS r FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id GROUPS BETWEEN 2 PRECEDING AND CURRENT ROW)"
} | $CLICKHOUSE_CLIENT
