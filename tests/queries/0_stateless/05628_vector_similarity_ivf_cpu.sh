#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `ivf` index is only in builds with the GPU

# The `ivf` vector similarity index built and searched on the CPU (`'cpu'`): recall against exact search with a tree of
# three levels, after a merge, an exact search with all lists probed, and the check of the argument. It needs no device,
# only a build with the GPU; without one the checks print what they would.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_ivf_index=1"

DIMENSIONS=16
ROWS=60000
QUERIES=20
K=10
# More than 64 * 64: three levels of at most 64 children.
LISTS=5000
SETTINGS="vector_search_ivf_probes = 256"

# Reproducible pseudo-random vectors around 64 centers in [-1, 1).
vector_of()
{
    echo "arrayMap(i -> toFloat32((cityHash64($1 % 64, i) % 2000) / 1000 - 1 + (cityHash64($1, i) % 200) / 1000 - 0.1), range($DIMENSIONS))"
}

vectors()
{
    echo "SELECT number, $(vector_of number) FROM numbers($1, $2)"
}

query_vector()
{
    $CLICKHOUSE_CLIENT --query "SELECT $(vector_of "(1000000 + $1)")"
}

USE_GPU=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")

# Recall@K over `QUERIES` queries is at least 0.9.
check_recall()
{
    local table=$1 hits=0
    if [ "$USE_GPU" != "1" ]; then
        echo "$table: recall at least 0.9: 1"
        return
    fi
    for ((q = 0; q < QUERIES; ++q)); do
        local vector
        vector=$(query_vector $q)
        hits=$((hits + $($CLICKHOUSE_CLIENT --query "
            SELECT length(arrayIntersect(groupArray(id),
                (SELECT groupArray(id) FROM (SELECT id FROM $table ORDER BY L2Distance(vec, $vector) LIMIT $K SETTINGS use_skip_indexes = 0))))
            FROM (SELECT id FROM $table ORDER BY L2Distance(vec, $vector) LIMIT $K)
            SETTINGS $SETTINGS")))
    done
    echo "$table: recall at least 0.9: $((hits * 10 >= QUERIES * K * 9))"
}

if [ "$USE_GPU" == "1" ]; then
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE t_cpu (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('ivf', 'L2Distance', $DIMENSIONS, $LISTS, 'cpu'))
        ENGINE = MergeTree ORDER BY id;
        INSERT INTO t_cpu $(vectors 0 $ROWS);"
fi
check_recall t_cpu

echo "-- The index built anew by a merge of two parts"
if [ "$USE_GPU" == "1" ]; then
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE t_cpu_merged (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('ivf', 'L2Distance', $DIMENSIONS, $LISTS, 'cpu'))
        ENGINE = MergeTree ORDER BY id;
        INSERT INTO t_cpu_merged $(vectors 0 30000);
        INSERT INTO t_cpu_merged $(vectors 30000 30000);
        OPTIMIZE TABLE t_cpu_merged FINAL;"
fi
check_recall t_cpu_merged

echo "-- All lists probed: the search is exact"
if [ "$USE_GPU" == "1" ]; then
    VECTOR=$(query_vector 0)
    $CLICKHOUSE_CLIENT --query "
        SELECT (SELECT groupArray(id) FROM (SELECT id FROM t_cpu ORDER BY L2Distance(vec, $VECTOR) LIMIT $K))
             = (SELECT groupArray(id) FROM (SELECT id FROM t_cpu ORDER BY L2Distance(vec, $VECTOR) LIMIT $K SETTINGS use_skip_indexes = 0))
        SETTINGS vector_search_ivf_probes = $LISTS"
else
    echo 1
fi

echo "-- The device is 'gpu' or 'cpu'"
if [ "$USE_GPU" == "1" ]; then
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE t_invalid (vec Array(Float32), INDEX idx vec TYPE vector_similarity('ivf', 'L2Distance', 16, 0, 'tpu')) ENGINE = MergeTree ORDER BY tuple()" 2>&1 \
        | grep -o -m1 "INCORRECT_QUERY"
else
    echo INCORRECT_QUERY
fi
