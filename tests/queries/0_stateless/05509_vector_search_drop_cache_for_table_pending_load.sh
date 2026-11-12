#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the fast test build has no vector similarity indexes, because it is built without the `usearch` library
# no-parallel: `SYSTEM ENABLE FAILPOINT` is process-wide, and the test counts the entries of the whole cache

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `SYSTEM DROP VECTOR SIMILARITY INDEX CACHE FOR TABLE <table>` must also cancel the insertion of an entry that a search is
# loading while the statement runs: otherwise the search caches the entry right after the statement returned. It must not
# cancel the insertions of entries of other tables.

FP=vector_similarity_index_cache_pause_before_insert

function cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP" 2>/dev/null
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS tab1;
    DROP TABLE IF EXISTS tab2;

    CREATE TABLE tab1
    (
        id Int32,
        vec Array(Float32),
        INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
    )
    ENGINE = MergeTree
    ORDER BY id;

    CREATE TABLE tab2
    (
        id Int32,
        vec Array(Float32),
        INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)
    )
    ENGINE = MergeTree
    ORDER BY id;

    INSERT INTO tab1 VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [0.0, 2.0]);
    INSERT INTO tab2 VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [0.0, 2.0]);
"

# `tab1` has a single part, and `idx` has a single index granule in it (its default `GRANULARITY` is 100000000), so the
# search looks up a single cache entry. With parallel replicas, also when automatic parallel replicas only collect
# statistics, the search can look up its entry a second time, after the statement, and cache it then.
SEARCH="SELECT id FROM tab1 ORDER BY L2Distance(vec, [0.0, 2.0]) LIMIT 2 FORMAT Null
    SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0"

# Starts the search on an empty cache, runs `SYSTEM DROP VECTOR SIMILARITY INDEX CACHE <target>` after the search loaded the
# entry of `tab1.idx` and before it cached it, lets the search finish, and prints how many entries are cached then.
function drop_during_search()
{
    local target=$1

    $CLICKHOUSE_CLIENT --query "SYSTEM DROP VECTOR SIMILARITY INDEX CACHE"
    $CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT $FP"
    $CLICKHOUSE_CLIENT --query "$SEARCH" &
    local search_pid=$!

    $CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT $FP PAUSE"
    $CLICKHOUSE_CLIENT --query "SYSTEM DROP VECTOR SIMILARITY INDEX CACHE $target"
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT $FP"
    wait $search_pid

    $CLICKHOUSE_CLIENT --query "SELECT '$target', value FROM system.metrics WHERE metric = 'VectorSimilarityIndexCacheCells'"
}

# Another table: the search caches the entry.
drop_during_search "FOR TABLE tab2"

# The table of the entry: the search does not cache it.
drop_during_search "FOR TABLE tab1"

$CLICKHOUSE_CLIENT --query "
    DROP TABLE tab1;
    DROP TABLE tab2;
"
