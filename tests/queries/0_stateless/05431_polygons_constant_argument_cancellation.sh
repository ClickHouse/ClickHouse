#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A constant geometry is used by every row of a block, so comparing a large constant polygon with many rows
# can take minutes in one block. The deadline must be observed between rows. The oracle is the outer `timeout`.

# A convex polygon of a million vertices (a parabola closed by a segment), a small square inside it, and a square
# that contains it (so that their union is small).
LARGE="[arrayMap(i -> (i / 500000 - 1, (i / 500000 - 1) * (i / 500000 - 1)), range(1000001))]"
SMALL="[[(x, 0.5), (x + 0.001, 0.5), (x + 0.001, 0.501), (x, 0.501)]]"
COVER="[[(x - 2, -1.), (x + 2, -1.), (x + 2, 2.), (x - 2, 2.)]]"
# A square with four million empty holes: no points, but every hole is visited.
HOLES="arrayConcat([[(-2., -2.), (2., -2.), (2., 2.), (-2., 2.)]], arrayMap(i -> []::Array(Tuple(Float64, Float64)), range(4000000)))"

# polygonsSymDifference is not checked: its result always contains the large polygon, so it races the memory limit.
for expression in \
    "countIf(polygonsWithinCartesian(${SMALL}, ${LARGE}))" \
    "sum(polygonsDistanceCartesian(${SMALL}, ${LARGE}))" \
    "countIf(polygonsEqualsCartesian(${SMALL}, ${LARGE}))" \
    "countIf(polygonsIntersectCartesian(${SMALL}, ${LARGE}))" \
    "countIf(geometryIntersectCartesian(${SMALL}::Polygon, ${LARGE}::Polygon))" \
    "sum(length(polygonsIntersectionCartesian(${SMALL}, ${LARGE})))" \
    "sum(length(polygonsUnionCartesian(${COVER}, ${LARGE})))" \
    "countIf(polygonsWithinCartesian(${SMALL}, ${HOLES}))"
do
    if timeout 30 ${CLICKHOUSE_CLIENT} --max_execution_time 1 --max_block_size 262144 --query "
            SELECT ${expression} FROM (SELECT number / 262144 - 0.5 AS x FROM numbers(262144))
        " 2>&1 | grep -q -F 'TIMEOUT_EXCEEDED'
    then
        echo 'stopped at the deadline'
    else
        echo 'still running after 30 seconds'
    fi
done
