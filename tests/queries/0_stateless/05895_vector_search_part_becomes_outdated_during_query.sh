#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the vector similarity index is not compiled into the Fast test build.

# Vector searches run concurrently with merges which make the part they read Outdated in the middle of the index analysis.
# A search which read some index granules of the part from the vector similarity index cache used to load the remaining
# granules into the cached object of the previous granule. This corrupted the cache entry and, when several searches did it
# at once, destroyed an index which was still being loaded.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 16384 rows with 64 rows per index granule: every part has 256 index granules, so a search reads the index of a part in 256
# steps and a merge is likely to finish in between. Searches which start right after a merge find a cold cache and wait for
# each other to load each granule, so they go through the granules of the new part together.
# The index stores the vectors as 'f32' because the default quantization ('bf16') cannot tell neighboring rows apart.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE tab
    (
        id Int32,
        vec Array(Float32),
        INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2, 'f32', 16, 128) GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS index_granularity = 64, index_granularity_bytes = 10485760;

    INSERT INTO tab SELECT number, [toFloat32(number), 0] FROM numbers(16384);
"

DURATION=10

function search_loop()
{
    local end=$((SECONDS + DURATION))
    while [ $SECONDS -lt $end ]; do
        # The row with id = 3000 is the only one at distance 0, so every search must return it.
        $CLICKHOUSE_CLIENT --use_query_cache 0 --query_plan_try_use_vector_search 1 -q "
            SELECT id FROM tab ORDER BY L2Distance(vec, [3000., 0]) LIMIT 1"
    done
}

function merge_loop()
{
    local end=$((SECONDS + DURATION))
    while [ $SECONDS -lt $end ]; do
        $CLICKHOUSE_CLIENT -q "OPTIMIZE TABLE tab FINAL"
    done
}

merge_loop &

{
    for _ in {1..16}; do
        search_loop &
    done
    wait
} | sort -u

wait

$CLICKHOUSE_CLIENT -q "DROP TABLE tab"
