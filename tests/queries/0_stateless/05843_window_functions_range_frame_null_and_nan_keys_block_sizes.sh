#!/usr/bin/env bash
# Tags: long

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./window_functions_block_sizes.lib
. "$CUR_DIR"/window_functions_block_sizes.lib

# RANGE frames with offsets over an ORDER BY key that holds NULL or NaN, sorted first and sorted last.
# A row with a NULL key gets the NULL rows of its partition as its frame, a row with a NaN key gets the NaN rows,
# and a row with a value gets only rows with values, however the sorted input is split into blocks.
# The keys: k is Nullable(Int32), f is Nullable(Float64) with NULL and NaN, g is Float64 with NaN where f has either.
# Partition 0 has only values, partition 1 has none, partitions 2 and 3 are one row each, without a value and
# with one, and partition 4 mixes runs of NULL keys, of NaN keys and of tied values.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (id UInt32, p UInt8, k Nullable(Int32), f Nullable(Float64), g Float64, v Int64) ENGINE = MergeTree ORDER BY id;
"
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t SELECT
        number,
        multiIf(number < 40, 0, number < 60, 1, number < 61, 2, number < 62, 3, 4) AS p,
        multiIf(p = 0, intDiv((number * 7) % 23, 2), p IN (1, 2), NULL, p = 3, 5, number % 5 < 2, NULL, intDiv((number * 7) % 47, 4)),
        multiIf(p = 0, ((number * 7) % 23) / 2, p IN (1, 2), NULL, p = 3, 5, number % 5 < 2, NULL, number % 5 = 2, nan, ((number * 7) % 47) / 4) AS f,
        ifNull(f, nan),
        (number * 13) % 29
    FROM numbers(200);
"

# The row order inside a peer group is not fixed, so only aggregates whose result does not depend on it.
aggregates="(count() OVER w, sum(v) OVER w, min(id) OVER w, max(id) OVER w) AS r"

{
check 'asc nulls first, around the current row' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k ASC NULLS FIRST RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING)"
check 'asc nulls last, around the current row' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k ASC NULLS LAST RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING)"
check 'desc nulls first, around the current row' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k DESC NULLS FIRST RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING)"
check 'desc nulls last, around the current row' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k DESC NULLS LAST RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING)"
check 'asc nulls first, ahead of the current row' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k ASC NULLS FIRST RANGE BETWEEN 1 FOLLOWING AND 3 FOLLOWING)"
check 'asc nulls last, ahead of the current row' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k ASC NULLS LAST RANGE BETWEEN 1 FOLLOWING AND 3 FOLLOWING)"
check 'desc nulls first, behind the current row' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k DESC NULLS FIRST RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING)"
check 'desc nulls last, behind the current row' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k DESC NULLS LAST RANGE BETWEEN 3 PRECEDING AND 1 PRECEDING)"
check 'asc nulls first, from the partition start' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)"
check 'asc nulls last, from the partition start' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k ASC NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)"
check 'desc nulls first, to the partition end' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k DESC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)"
check 'desc nulls last, to the partition end' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k DESC NULLS LAST RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)"
check 'asc nulls first, from an offset to the current row' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k ASC NULLS FIRST RANGE BETWEEN 2 PRECEDING AND CURRENT ROW)"
check 'asc nulls last, from the current row to an offset' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY k ASC NULLS LAST RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING)"
check 'nullable float key asc nulls first' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY f ASC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
check 'nullable float key asc nulls last' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY f ASC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
check 'nullable float key desc nulls first' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY f DESC NULLS FIRST RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING)"
check 'nullable float key desc nulls last' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY f DESC NULLS LAST RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING)"
check 'float key asc nulls first' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY g ASC NULLS FIRST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
check 'float key asc nulls last' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY g ASC NULLS LAST RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
check 'float key desc nulls first' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY g DESC NULLS FIRST RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING)"
check 'float key desc nulls last' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY g DESC NULLS LAST RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING)"
check 'float key asc nulls first, from the partition start' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY g ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING)"
check 'float key desc nulls last, to the partition end' "SELECT p, id, $aggregates FROM t WINDOW w AS (PARTITION BY p ORDER BY g DESC NULLS LAST RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)"
} | $CLICKHOUSE_CLIENT
