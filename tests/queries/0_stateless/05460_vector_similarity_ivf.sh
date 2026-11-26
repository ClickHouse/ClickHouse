#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the `vector_similarity` index needs usearch

# Recall of the `ivf` vector similarity index against exact search: both distance functions, after reload and merge, all
# lists probed. The vectors are clustered, as real data are.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_WITHOUT_IVF="$CLICKHOUSE_CLIENT"
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_ivf_index=1"

DIMENSIONS=32
ROWS=30000
QUERIES=20
K=10
LISTS=64
SETTINGS="vector_search_ivf_probes = 16"

# Reproducible pseudo-random vectors around 64 centers in [-1, 1), as an IVF index expects of real data: uniformly random
# vectors have no clusters, and their nearest neighbours are spread over all lists.
vector_of()
{
    echo "arrayMap(i -> toFloat32((cityHash64($1 % 64, i) % 2000) / 1000 - 1 + (cityHash64($1, i) % 200) / 1000 - 0.1), range($DIMENSIONS))"
}

vectors()
{
    local from=$1 count=$2
    echo "SELECT number, $(vector_of number) FROM numbers($from, $count)"
}

query_vector()
{
    $CLICKHOUSE_CLIENT --query "SELECT $(vector_of "(1000000 + $1)")"
}

METHOD="'ivf'"
METHOD_PARAMETERS="$DIMENSIONS, $LISTS"

# Recall@K over `QUERIES` queries is at least 0.9, and the index is used. The settings go on the outer query: the plan
# is optimized by the settings of the query, not of a subquery.
check_recall()
{
    local table=$1 distance=$2 settings=$3 hits=0
    for ((q = 0; q < QUERIES; ++q)); do
        local vector
        vector=$(query_vector $q)
        hits=$((hits + $($CLICKHOUSE_CLIENT --query "
            SELECT length(arrayIntersect(groupArray(id),
                (SELECT groupArray(id) FROM (SELECT id FROM $table ORDER BY $distance(vec, $vector) LIMIT $K SETTINGS use_skip_indexes = 0))))
            FROM (SELECT id FROM $table ORDER BY $distance(vec, $vector) LIMIT $K)
            SETTINGS $settings")))
    done
    local uses_index
    uses_index=$($CLICKHOUSE_CLIENT --query "
        EXPLAIN indexes = 1 SELECT id FROM $table ORDER BY $distance(vec, $(query_vector 0)) LIMIT $K SETTINGS $settings" | grep -c "Name: idx")
    echo "$table $distance: recall at least 0.9: $((hits * 10 >= QUERIES * K * 9)), index used: $((uses_index > 0))"
}

for distance in L2Distance cosineDistance; do
    table="t_${distance}"
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE $table (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity($METHOD, '$distance', $METHOD_PARAMETERS))
        ENGINE = MergeTree ORDER BY id;
        INSERT INTO $table $(vectors 0 $ROWS);"
    check_recall "$table" "$distance" "$SETTINGS"
done

echo "-- The index read back from disk"
$CLICKHOUSE_CLIENT --query "DETACH TABLE t_L2Distance; ATTACH TABLE t_L2Distance;"
check_recall t_L2Distance L2Distance "$SETTINGS"

echo "-- The index built anew by a merge of two parts"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_merged (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity($METHOD, 'L2Distance', $METHOD_PARAMETERS))
    ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_merged $(vectors 0 15000);
    INSERT INTO t_merged $(vectors 15000 15000);
    OPTIMIZE TABLE t_merged FINAL;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merged' AND active;"
check_recall t_merged L2Distance "$SETTINGS"

echo "-- All lists probed: the search is exact"
VECTOR=$(query_vector 0)
$CLICKHOUSE_CLIENT --query "
    SELECT (SELECT groupArray(id) FROM (SELECT id FROM t_L2Distance ORDER BY L2Distance(vec, $VECTOR) LIMIT $K))
         = (SELECT groupArray(id) FROM (SELECT id FROM t_L2Distance ORDER BY L2Distance(vec, $VECTOR) LIMIT $K SETTINGS use_skip_indexes = 0))
    SETTINGS vector_search_ivf_probes = $LISTS"

echo "-- Invalid arguments"
check_error()
{
    $CLICKHOUSE_CLIENT --query "CREATE TABLE t_invalid (vec Array(Float32), INDEX idx vec TYPE vector_similarity($1)) ENGINE = MergeTree ORDER BY tuple()" 2>&1 \
        | grep -o -m1 "INCORRECT_DATA\|INCORRECT_QUERY\|ILLEGAL_COLUMN"
}
check_error "'ivf', 'L2Distance', 32, 64, 1" INCORRECT_QUERY
check_error "'ivf', 'L2Distance', 32, 'f32'" INCORRECT_QUERY
check_error "'ivf', 'dotProduct', 32" INCORRECT_DATA
check_error "'ivf', 'L2Distance', 0" INCORRECT_DATA

echo "-- The experimental setting"
$CLICKHOUSE_CLIENT_WITHOUT_IVF --query "
    CREATE TABLE t_disabled (vec Array(Float32), INDEX idx vec TYPE vector_similarity('ivf', 'L2Distance', 32)) ENGINE = MergeTree ORDER BY tuple()" 2>&1 \
    | grep -o -m1 "SUPPORT_IS_DISABLED"
$CLICKHOUSE_CLIENT_WITHOUT_IVF --query "
    CREATE TABLE t_disabled (vec Array(Float32)) ENGINE = MergeTree ORDER BY tuple();
    ALTER TABLE t_disabled ADD INDEX idx vec TYPE vector_similarity('ivf', 'L2Distance', 32);" 2>&1 \
    | grep -o -m1 "SUPPORT_IS_DISABLED"
