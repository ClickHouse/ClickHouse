#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An aggregate function over a sliding window frame must return what the same function returns over the rows of
# the frame, for any frame length and any split of the input into blocks. Every query counts the rows where the two
# differ, so these numbers must be 0; the floats query also counts its NaN and infinite results.

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t (i UInt32, p UInt8, k UInt32, v Int64, nv Nullable(Int64), f Float64, g Float64) ENGINE = MergeTree ORDER BY i;
"

# Partitions of 1, 15, 16, 17, 255, 256, 257 and 1200 rows.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO t SELECT
        number,
        multiIf(number < 1, 0, number < 16, 1, number < 32, 2, number < 49, 3, number < 304, 4, number < 560, 5, number < 817, 6, 7),
        intDiv(number, 3),
        (number * 7919) % 1000,
        if(number % 5 = 0, NULL, (number * 7919) % 1000),
        ((number * 7919) % 1000) / 7,
        multiIf(number = 100, nan, number = 1000, inf, number = 1450, -inf, ((number * 7919) % 1000) / 7)
    FROM numbers(2017);
"

# The condition that a floating-point value differs from the expected one: NaN matches only NaN, an infinity only the same
# infinity, and finite values match within the relative tolerance.
function float_differs()
{
    local value=$1
    local expected=$2
    local tolerance=$3
    echo "NOT ((isNaN($value) AND isNaN($expected)) OR $value = $expected
        OR (isFinite($value) AND isFinite($expected) AND abs($value - $expected) <= $tolerance * greatest(abs($expected), 1)))"
}

# Frames in rows over a unique key: the order of the rows in a frame is fixed, so the first and the last value are checked too.
function check_rows()
{
    local name=$1
    local frame=$2
    for block_size in 7 333 65505
    do
        echo "
            SELECT '$name, block size $block_size', count(),
                countIf(s != arraySum(a)), countIf(mn != arrayMin(a)), countIf(mx != arrayMax(a)),
                countIf($(float_differs av 'arraySum(a) / length(a)' 0)),
                countIf(si != arraySum(arrayFilter(x -> x % 3 = 0, a))), countIf(st != arraySum(a)),
                countIf(ifNull(sn, 0) != arraySum(an)), countIf(fi != a[1]), countIf(la != a[-1])
            FROM
            (
                SELECT
                    sum(v) OVER w AS s, min(v) OVER w AS mn, max(v) OVER w AS mx, avg(v) OVER w AS av,
                    sumIf(v, v % 3 = 0) OVER w AS si, finalizeAggregation(sumState(v) OVER w) AS st, sum(nv) OVER w AS sn,
                    any(v) OVER w AS fi, anyLast(v) OVER w AS la,
                    groupArray(v) OVER w AS a, groupArray(nv) OVER w AS an
                FROM t
                WINDOW w AS (PARTITION BY p ORDER BY i $frame)
            )
            SETTINGS max_block_size = $block_size, window_sliding_aggregation_min_frame_rows = 1;"
    done
}

# Frames over a key with ties: the order of the rows inside a peer group is not fixed, so only the values that do not depend on it.
function check_peers()
{
    local name=$1
    local frame=$2
    for block_size in 7 333 65505
    do
        echo "
            SELECT '$name, block size $block_size', count(),
                countIf(s != arraySum(a)), countIf(mn != arrayMin(a)), countIf(mx != arrayMax(a)),
                countIf($(float_differs av 'arraySum(a) / length(a)' 0)),
                countIf(si != arraySum(arrayFilter(x -> x % 3 = 0, a))), countIf(st != arraySum(a)),
                countIf(ifNull(sn, 0) != arraySum(an))
            FROM
            (
                SELECT
                    sum(v) OVER w AS s, min(v) OVER w AS mn, max(v) OVER w AS mx, avg(v) OVER w AS av,
                    sumIf(v, v % 3 = 0) OVER w AS si, finalizeAggregation(sumState(v) OVER w) AS st, sum(nv) OVER w AS sn,
                    groupArray(v) OVER w AS a, groupArray(nv) OVER w AS an
                FROM t
                WINDOW w AS (PARTITION BY p ORDER BY k $frame)
            )
            SETTINGS max_block_size = $block_size, window_sliding_aggregation_min_frame_rows = 1;"
    done
}

# One partition of 140000 consecutive numbers: the frame of a row is the numbers from low to high.
function check_series()
{
    local name=$1
    local frame=$2
    local low=$3
    local high=$4
    local settings=$5
    echo "
        SELECT '$name', count(),
            countIf(s != intDiv(($low + $high) * ($high - $low + 1), 2)), countIf(mn != $low), countIf(mx != $high)
        FROM
        (
            SELECT number, sum(number) OVER w AS s, min(number) OVER w AS mn, max(number) OVER w AS mx
            FROM numbers(140000)
            WINDOW w AS (ORDER BY number $frame)
        )
        SETTINGS $settings;"
}

# Functions over floating-point values can differ from the value over the rows of the frame in the last digits.
# `g` has a NaN and two infinities, so some of its frames sum to NaN or to an infinity.
function check_floats()
{
    for block_size in 7 65505
    do
        echo "
            SELECT 'floats, block size $block_size', count(),
                countIf($(float_differs sd "arrayReduce('stddevPop', a)" 1e-9)),
                countIf($(float_differs co "arrayReduce('corr', a, b)" 1e-9)),
                countIf($(float_differs sf 'arraySum(c)' 1e-9)),
                countIf($(float_differs sg 'arraySum(d)' 1e-9)),
                countIf(isNaN(co)), countIf(isNaN(sg)), countIf(sg = inf), countIf(sg = -inf)
            FROM
            (
                SELECT
                    stddevPop(v) OVER w AS sd, corr(v, i) OVER w AS co, sum(f) OVER w AS sf, sum(g) OVER w AS sg,
                    groupArray(v) OVER w AS a, groupArray(i) OVER w AS b, groupArray(f) OVER w AS c, groupArray(g) OVER w AS d
                FROM t
                WINDOW w AS (PARTITION BY p ORDER BY i ROWS BETWEEN 300 PRECEDING AND 290 FOLLOWING)
            )
            SETTINGS max_block_size = $block_size, window_sliding_aggregation_min_frame_rows = 1;"
    done
}

{
check_rows 'rows 40 preceding 30 following' 'ROWS BETWEEN 40 PRECEDING AND 30 FOLLOWING'
check_rows 'rows 300 preceding 290 following' 'ROWS BETWEEN 300 PRECEDING AND 290 FOLLOWING'
check_rows 'rows current row unbounded following' 'ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING'
check_rows 'rows 5 following 400 following' 'ROWS BETWEEN 5 FOLLOWING AND 400 FOLLOWING'
check_rows 'rows 600 preceding 100 preceding' 'ROWS BETWEEN 600 PRECEDING AND 100 PRECEDING'

check_peers 'range 100 preceding 30 following' 'RANGE BETWEEN 100 PRECEDING AND 30 FOLLOWING'
check_peers 'range current row unbounded following' 'RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING'
check_peers 'groups 50 preceding 20 following' 'GROUPS BETWEEN 50 PRECEDING AND 20 FOLLOWING'

check_series 'series current row unbounded following, block size 1000' 'ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING' \
    'number' '139999' 'max_block_size = 1000, window_sliding_aggregation_min_frame_rows = 1'
check_series 'series current row unbounded following' 'ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING' \
    'number' '139999' 'window_sliding_aggregation_min_frame_rows = 1'
check_series 'series current row unbounded following, default setting' 'ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING' \
    'number' '139999' 'max_block_size = 65505'
check_series 'series 5000 preceding 4999 following, block size 1000' 'ROWS BETWEEN 5000 PRECEDING AND 4999 FOLLOWING' \
    'if(number < 5000, 0, number - 5000)' 'least(number + 4999, 139999)' 'max_block_size = 1000, window_sliding_aggregation_min_frame_rows = 1'
check_series 'series 5000 preceding 4999 following' 'ROWS BETWEEN 5000 PRECEDING AND 4999 FOLLOWING' \
    'if(number < 5000, 0, number - 5000)' 'least(number + 4999, 139999)' 'window_sliding_aggregation_min_frame_rows = 1'
check_series 'series 5000 preceding 4999 following, default setting' 'ROWS BETWEEN 5000 PRECEDING AND 4999 FOLLOWING' \
    'if(number < 5000, 0, number - 5000)' 'least(number + 4999, 139999)' 'max_block_size = 65505'

check_floats
} | $CLICKHOUSE_CLIENT
