#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./window_functions_block_sizes.lib
. "$CUR_DIR"/window_functions_block_sizes.lib

create_table

# The same aggregates over every frame kind and bound kind, with Nullable and -State arguments among them.
# The row order inside a peer group is not fixed, so frames over peer groups use only aggregates whose
# result does not depend on the order of rows.
aggregates="(sum(v) OVER w, sum(k) OVER w, toString(round(avg(v) OVER w, 6)), min(k) OVER w, max(v) OVER w, count() OVER w, count(k) OVER w, any(v) OVER w, anyLast(k) OVER w, argMax(id, v) OVER w, groupArray(v) OVER w, uniq(k) OVER w, finalizeAggregation(sumState(v) OVER w), finalizeAggregation(maxState(k) OVER w)) AS r"
unordered_aggregates="(sum(v) OVER w, sum(k) OVER w, toString(round(avg(v) OVER w, 6)), min(k) OVER w, max(v) OVER w, count() OVER w, count(k) OVER w, min(v) OVER w, max(k) OVER w, argMax(v, id) OVER w, arraySort(groupArray(v) OVER w), uniq(k) OVER w, finalizeAggregation(sumState(v) OVER w), finalizeAggregation(maxState(k) OVER w)) AS r"

{
check 'rows from the partition start' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
check 'rows from the partition start to a preceding row' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING)"
check 'rows sliding' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING)"
check 'rows ahead of the current row' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN 1 FOLLOWING AND 3 FOLLOWING)"
check 'rows behind the current row' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)"
check 'rows to the partition end' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k, id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)"
check 'range default frame' "SELECT p, id, $unordered_aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k)"
check 'range offsets' "SELECT p, id, $unordered_aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY v RANGE BETWEEN 3 PRECEDING AND 3 FOLLOWING)"
check 'range ahead of the current row' "SELECT p, id, $unordered_aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY v RANGE BETWEEN 1 FOLLOWING AND 5 FOLLOWING)"
check 'range to the partition end' "SELECT p, id, $unordered_aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)"
check 'groups sliding' "SELECT p, id, $unordered_aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
check 'groups from the partition start to a preceding group' "SELECT p, id, $unordered_aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)"
check 'whole partition' "SELECT p, id, $unordered_aggregates FROM t WINDOW w AS (PARTITION BY p)"
} | $CLICKHOUSE_CLIENT
