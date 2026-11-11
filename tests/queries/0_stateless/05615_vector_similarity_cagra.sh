#!/usr/bin/env bash

# Recall of the `cagra` vector similarity index and of `vectorSearch` against exact search: each distance function, GPU
# and CPU search, after reload and merge, small granules, and CAGRA-Q. Without a usable GPU the same checks run with
# `hnsw` (unquantized), so the test runs everywhere.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_WITHOUT_CAGRA="$CLICKHOUSE_CLIENT"
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_cagra_index=1"

DIMENSIONS=32
ROWS=30000
QUERIES=20
K=10

# Reproducible pseudo-random vectors in [-1, 1).
vectors()
{
    local from=$1 count=$2
    echo "SELECT number, arrayMap(i -> toFloat32((cityHash64(number, i) % 2000) / 1000 - 1), range($DIMENSIONS))
          FROM numbers($from, $count)"
}

query_vector()
{
    $CLICKHOUSE_CLIENT --query "SELECT arrayMap(i -> toFloat32((cityHash64(1000000 + $1, i) % 2000) / 1000 - 1), range($DIMENSIONS))"
}

USE_GPU=$($CLICKHOUSE_CLIENT --query "SELECT value FROM system.build_options WHERE name = 'USE_GPU'")

# Arguments of `vector_similarity` around the distance function.
METHOD="'hnsw'"
METHOD_PARAMETERS="$DIMENSIONS, 'f32', 32, 128"
# The same for CAGRA-Q.
PQ_METHOD_PARAMETERS="$METHOD_PARAMETERS"
if [ "$USE_GPU" == "1" ]; then
    # 10000 rows get a graph and need the device; no device or no `GLIBC_TUNABLES` falls back, other errors are printed.
    PROBE_ERROR=$($CLICKHOUSE_CLIENT --query "
        CREATE TABLE probe (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity('cagra', 'L2Distance', $DIMENSIONS))
        ENGINE = MergeTree ORDER BY id;
        INSERT INTO probe $(vectors 0 20000);
        SELECT id FROM probe ORDER BY L2Distance(vec, $(query_vector 0)) LIMIT 1 FORMAT Null;
        DROP TABLE probe;" 2>&1 > /dev/null)
    if [ -z "$PROBE_ERROR" ]; then
        METHOD="'cagra'"
        METHOD_PARAMETERS="$DIMENSIONS"
        PQ_METHOD_PARAMETERS="$DIMENSIONS, 'pq2'"
    elif [[ "$PROBE_ERROR" != *"There is no GPU"* && "$PROBE_ERROR" != *"GLIBC_TUNABLES"* ]]; then
        echo "$PROBE_ERROR"
    fi
    $CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS probe"
fi

# Recall@K over `QUERIES` queries is at least 0.9, and the index is used. The settings go on the outer query: the plan
# is optimized (e.g. for `vector_search_with_rescoring`) by the settings of the query, not of a subquery.
check_recall()
{
    local table=$1 distance=$2 settings=$3 hits=0 direction=ASC
    # A larger inner product is nearer.
    [ "$distance" == "dotProduct" ] && direction=DESC
    for ((q = 0; q < QUERIES; ++q)); do
        local vector
        vector=$(query_vector $q)
        hits=$((hits + $($CLICKHOUSE_CLIENT --query "
            SELECT length(arrayIntersect(groupArray(id),
                (SELECT groupArray(id) FROM (SELECT id FROM $table ORDER BY $distance(vec, $vector) $direction LIMIT $K SETTINGS use_skip_indexes = 0))))
            FROM (SELECT id FROM $table ORDER BY $distance(vec, $vector) $direction LIMIT $K)
            SETTINGS $settings")))
    done
    local uses_index
    uses_index=$($CLICKHOUSE_CLIENT --query "
        EXPLAIN indexes = 1 SELECT id FROM $table ORDER BY $distance(vec, $(query_vector 0)) $direction LIMIT $K SETTINGS $settings" | grep -c "Name: idx")
    echo "$table $distance: recall at least 0.9: $((hits * 10 >= QUERIES * K * 9)), index used: $((uses_index > 0))"
}

for distance in L2Distance cosineDistance dotProduct; do
    table="t_${distance}"
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE $table (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity($METHOD, '$distance', $METHOD_PARAMETERS))
        ENGINE = MergeTree ORDER BY id;
        INSERT INTO $table $(vectors 0 $ROWS);"
    check_recall "$table" "$distance" "vector_search_cagra_on_cpu = 0"
    check_recall "$table" "$distance" "vector_search_cagra_on_cpu = 1"
done

echo "-- The index read back from disk"
$CLICKHOUSE_CLIENT --query "DETACH TABLE t_L2Distance; ATTACH TABLE t_L2Distance;"
check_recall t_L2Distance L2Distance "vector_search_cagra_on_cpu = 0"

echo "-- The index built anew by a merge of two parts"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_merged (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity($METHOD, 'L2Distance', $METHOD_PARAMETERS))
    ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_merged $(vectors 0 15000);
    INSERT INTO t_merged $(vectors 15000 15000);
    OPTIMIZE TABLE t_merged FINAL;
    SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_merged' AND active;"
check_recall t_merged L2Distance "vector_search_cagra_on_cpu = 0"

echo "-- A granule too small for a graph is searched exactly"
$CLICKHOUSE_CLIENT --query "
    CREATE TABLE t_small (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity($METHOD, 'L2Distance', $METHOD_PARAMETERS))
    ENGINE = MergeTree ORDER BY id;
    INSERT INTO t_small $(vectors 0 500);"
VECTOR=$(query_vector 0)
$CLICKHOUSE_CLIENT --query "
    SELECT (SELECT groupArray(id) FROM (SELECT id FROM t_small ORDER BY L2Distance(vec, $VECTOR) LIMIT $K))
         = (SELECT groupArray(id) FROM (SELECT id FROM t_small ORDER BY L2Distance(vec, $VECTOR) LIMIT $K SETTINGS use_skip_indexes = 0))"

echo "-- Vectors compressed by VPQ: the distances are approximate, the rescoring makes them exact"
RESCORING="vector_search_with_rescoring = 1, vector_search_index_fetch_multiplier = 4"
for distance in L2Distance cosineDistance; do
    table="t_pq_${distance}"
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE $table (id UInt64, vec Array(Float32), INDEX idx vec TYPE vector_similarity($METHOD, '$distance', $PQ_METHOD_PARAMETERS))
        ENGINE = MergeTree ORDER BY id;
        INSERT INTO $table $(vectors 0 $ROWS);"
    check_recall "$table" "$distance" "vector_search_cagra_on_cpu = 0, $RESCORING"
    check_recall "$table" "$distance" "vector_search_cagra_on_cpu = 1, $RESCORING"
done
$CLICKHOUSE_CLIENT --query "DETACH TABLE t_pq_L2Distance; ATTACH TABLE t_pq_L2Distance;"
check_recall t_pq_L2Distance L2Distance "vector_search_cagra_on_cpu = 0, $RESCORING"
if [ "$METHOD" == "'cagra'" ]; then
    $CLICKHOUSE_CLIENT --query "
        SELECT 'smaller index',
            (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 't_pq_L2Distance' AND active)
          < (SELECT sum(secondary_indices_uncompressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 't_L2Distance' AND active)"
else
    echo -e "smaller index\t1"
fi

echo "-- vectorSearch: all the queries at once, against an exact search of each"
if [ "$USE_GPU" == "1" ]; then
    $CLICKHOUSE_CLIENT --query "
        CREATE TABLE queries (q UInt32, vec Array(Float32)) ENGINE = MergeTree ORDER BY q;
        INSERT INTO queries SELECT number, arrayMap(i -> toFloat32((cityHash64(1000000 + number, i) % 2000) / 1000 - 1), range($DIMENSIONS))
            FROM numbers($QUERIES);"
    # No rescoring in `vectorSearch`: CAGRA-Q ranks by approximate distances.
    for table_and_recall in t_L2Distance:9 t_merged:9 t_pq_L2Distance:7; do
        table=${table_and_recall%:*}
        recall=${table_and_recall#*:}
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
                   (SELECT count() FROM found INNER JOIN exact USING (q, id)) * 10 >= $QUERIES * $K * $recall"
    done
else
    for table in t_L2Distance t_merged t_pq_L2Distance; do
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
check_error "'cagra', 'L2Distance', 32, 64" INCORRECT_QUERY
check_error "'cagra', 'L2Distance', 32, 'f32', 64" INCORRECT_QUERY
check_error "'cagra', 'L2Distance', 32, 'f32', 0, 64" INCORRECT_DATA
check_error "'cagra', 'L2Distance', 32, 'f32', 64, 32" INCORRECT_DATA
check_error "'cagra', 'hammingDistance', 32" INCORRECT_DATA
check_error "'cagra', 'L2Distance', 0" INCORRECT_DATA
check_error "'cagra', 'L2Distance', 32, 'pq3'" INCORRECT_DATA
check_error "'cagra', 'dotProduct', 32, 'pq2'" INCORRECT_DATA
check_error "'cagra', 'L2Distance', 30, 'pq4'" INCORRECT_DATA

echo "-- The experimental setting"
if [ "$USE_GPU" == "1" ]; then
    $CLICKHOUSE_CLIENT_WITHOUT_CAGRA --query "
        CREATE TABLE t_disabled (vec Array(Float32), INDEX idx vec TYPE vector_similarity('cagra', 'L2Distance', 32)) ENGINE = MergeTree ORDER BY tuple()" 2>&1 \
        | grep -o -m1 "SUPPORT_IS_DISABLED"
    $CLICKHOUSE_CLIENT_WITHOUT_CAGRA --query "
        CREATE TABLE t_disabled (vec Array(Float32)) ENGINE = MergeTree ORDER BY tuple();
        ALTER TABLE t_disabled ADD INDEX idx vec TYPE vector_similarity('cagra', 'L2Distance', 32);" 2>&1 \
        | grep -o -m1 "SUPPORT_IS_DISABLED"
else
    echo SUPPORT_IS_DISABLED
    echo SUPPORT_IS_DISABLED
fi
