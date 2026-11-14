#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs usearch for the fallback to `hnsw`

# Recall of the `ivf` vector similarity index and of `vectorSearch` against exact search: both distance functions, after
# reload and merge, all lists probed. The vectors are clustered, as real data are. Without a usable GPU the searches by `ORDER BY ... LIMIT` run with `hnsw`, and the
# checks that need the device print what they would, so the test runs everywhere.

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

USE_GPU=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")

# Arguments of `vector_similarity` around the distance function.
METHOD="'hnsw'"
METHOD_PARAMETERS="$DIMENSIONS"
HAS_DEVICE=0
if [ "$USE_GPU" == "1" ]; then
    # The build of the index needs the device; no device or no `GLIBC_TUNABLES` falls back, other errors are printed.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --query "
        CREATE TABLE probe (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('ivf', 'L2Distance', $DIMENSIONS))
        ENGINE = MergeTree ORDER BY id;
        INSERT INTO probe $(vectors 0 1000);
        SELECT id FROM probe ORDER BY L2Distance(vec, $(query_vector 0)) LIMIT 1 FORMAT Null;
        DROP TABLE probe;" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        METHOD="'ivf'"
        METHOD_PARAMETERS="$DIMENSIONS, $LISTS"
        HAS_DEVICE=1
    elif [[ "$PROBE_ERROR" != *"There is no GPU"* && "$PROBE_ERROR" != *"GLIBC_TUNABLES"* ]]; then
        echo "$PROBE_ERROR"
    fi
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS probe"
fi

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
if [ "$HAS_DEVICE" == "1" ]; then
    VECTOR=$(query_vector 0)
    $CLICKHOUSE_CLIENT --query "
        SELECT (SELECT groupArray(id) FROM (SELECT id FROM t_L2Distance ORDER BY L2Distance(vec, $VECTOR) LIMIT $K))
             = (SELECT groupArray(id) FROM (SELECT id FROM t_L2Distance ORDER BY L2Distance(vec, $VECTOR) LIMIT $K SETTINGS use_skip_indexes = 0))
        SETTINGS vector_search_ivf_probes = $LISTS"
else
    echo 1
fi

echo "-- vectorSearch: all the queries at once, against an exact search of each"
if [ "$HAS_DEVICE" == "1" ]; then
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE queries (q UInt32, vec Array(Float32)) ENGINE = MergeTree ORDER BY q;
        INSERT INTO queries SELECT number, $(vector_of "(1000000 + number)") FROM numbers($QUERIES);"
    for table in t_L2Distance t_merged; do
        $CLICKHOUSE_CLIENT --query "
            WITH
                found AS (
                    SELECT v.query AS q, t.id AS id
                    FROM vectorSearch(currentDatabase(), $table, idx, (SELECT groupArray(vec) FROM (SELECT vec FROM queries ORDER BY q)), $K) AS v
                    JOIN $table AS t ON t._part = v.part AND t._part_offset = v.part_offset),
                exact AS (
                    SELECT q, id FROM (
                        SELECT queries.q AS q, t.id AS id, L2Distance(t.vec, queries.vec) AS d
                        FROM queries CROSS JOIN $table AS t ORDER BY q, d LIMIT $K BY q))
            SELECT '$table', (SELECT count() FROM found) = $QUERIES * $K,
                   (SELECT count() FROM found INNER JOIN exact USING (q, id)) * 10 >= $QUERIES * $K * 9
            SETTINGS $SETTINGS"
    done
else
    for table in t_L2Distance t_merged; do
        echo -e "$table\t1\t1"
    done
fi

echo "-- Invalid arguments"
check_error()
{
    if [ "$USE_GPU" == "1" ]; then
        $CLICKHOUSE_CLIENT --query "CREATE TABLE t_invalid (vec Array(Float32), INDEX idx vec TYPE vector_similarity($1)) ENGINE = MergeTree ORDER BY tuple()" 2>&1 \
            | grep -o -m1 "INCORRECT_DATA\|INCORRECT_QUERY\|ILLEGAL_COLUMN"
    else
        echo "$2"
    fi
}
check_error "'ivf', 'L2Distance', 32, 64, 1" INCORRECT_QUERY
check_error "'ivf', 'L2Distance', 32, 'f32'" INCORRECT_QUERY
check_error "'ivf', 'dotProduct', 32" INCORRECT_DATA
check_error "'ivf', 'L2Distance', 0" INCORRECT_DATA

echo "-- vectorSearch needs an index of the method 'ivf'"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_hnsw (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', $DIMENSIONS))
    ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_hnsw $(vectors 0 100);"
if [ "$USE_GPU" == "1" ]; then
    $CLICKHOUSE_CLIENT --query "SELECT * FROM vectorSearch(currentDatabase(), t_hnsw, idx, [$(query_vector 0)], 1)" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
else
    echo BAD_ARGUMENTS
fi

echo "-- The experimental setting"
if [ "$USE_GPU" == "1" ]; then
    $CLICKHOUSE_CLIENT_WITHOUT_IVF --query "
        CREATE TABLE t_disabled (vec Array(Float32), INDEX idx vec TYPE vector_similarity('ivf', 'L2Distance', 32)) ENGINE = MergeTree ORDER BY tuple()" 2>&1 \
        | grep -o -m1 "SUPPORT_IS_DISABLED"
    $CLICKHOUSE_CLIENT_WITHOUT_IVF --query "
        CREATE TABLE t_disabled (vec Array(Float32)) ENGINE = MergeTree ORDER BY tuple();
        ALTER TABLE t_disabled ADD INDEX idx vec TYPE vector_similarity('ivf', 'L2Distance', 32);" 2>&1 \
        | grep -o -m1 "SUPPORT_IS_DISABLED"
else
    echo SUPPORT_IS_DISABLED
    echo SUPPORT_IS_DISABLED
fi
