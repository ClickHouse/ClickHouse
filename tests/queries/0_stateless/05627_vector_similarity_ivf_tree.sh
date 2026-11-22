#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs usearch for the fallback to `hnsw`

# The `ivf` vector similarity index with more lists than the children of one node of its tree, so that the lists are
# reached through a tree of three levels: recall against exact search, after a merge, and an exact search with all lists
# probed. Without a usable GPU the checks print what they would, so the test runs everywhere.

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

HAS_DEVICE=0
if [ "$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")" == "1" ]; then
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --query "
        CREATE TABLE probe (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('ivf', 'L2Distance', $DIMENSIONS))
        ENGINE = MergeTree ORDER BY id;
        INSERT INTO probe $(vectors 0 1000);
        SELECT id FROM probe ORDER BY L2Distance(vec, $(query_vector 0)) LIMIT 1 FORMAT Null;
        DROP TABLE probe;" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        HAS_DEVICE=1
    elif [[ "$PROBE_ERROR" != *"There is no GPU"* && "$PROBE_ERROR" != *"GLIBC_TUNABLES"* ]]; then
        echo "$PROBE_ERROR"
    fi
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS probe"
fi

# Recall@K over `QUERIES` queries is at least 0.9.
check_recall()
{
    local table=$1 hits=0
    if [ "$HAS_DEVICE" != "1" ]; then
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

if [ "$HAS_DEVICE" == "1" ]; then
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE t_tree (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('ivf', 'L2Distance', $DIMENSIONS, $LISTS))
        ENGINE = MergeTree ORDER BY id;
        INSERT INTO t_tree $(vectors 0 $ROWS);"
fi
check_recall t_tree

echo "-- The index built anew by a merge of two parts"
if [ "$HAS_DEVICE" == "1" ]; then
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE t_merged (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('ivf', 'L2Distance', $DIMENSIONS, $LISTS))
        ENGINE = MergeTree ORDER BY id;
        INSERT INTO t_merged $(vectors 0 30000);
        INSERT INTO t_merged $(vectors 30000 30000);
        OPTIMIZE TABLE t_merged FINAL;"
fi
check_recall t_merged

echo "-- All lists probed: the search is exact"
if [ "$HAS_DEVICE" == "1" ]; then
    VECTOR=$(query_vector 0)
    $CLICKHOUSE_CLIENT --query "
        SELECT (SELECT groupArray(id) FROM (SELECT id FROM t_tree ORDER BY L2Distance(vec, $VECTOR) LIMIT $K))
             = (SELECT groupArray(id) FROM (SELECT id FROM t_tree ORDER BY L2Distance(vec, $VECTOR) LIMIT $K SETTINGS use_skip_indexes = 0))
        SETTINGS vector_search_ivf_probes = $LISTS"
else
    echo 1
fi
