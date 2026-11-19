#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: the pause failpoint needs a build with libfiu, and the vector index needs usearch.
# no-parallel: the failpoint and the prewarm thread-pool metrics are server-wide.

# `SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE` builds a thread pool counted by
# `MergeTreePrewarmVectorSimilarityIndexCacheThreads`, `MergeTreePrewarmVectorSimilarityIndexCacheThreadsActive` and
# `MergeTreePrewarmVectorSimilarityIndexCacheThreadsScheduled`. The pool is destroyed when the command returns,
# so the metrics are read while the only worker is paused inside `loadVectorSimilarityIndexToCache`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

FP=vector_similarity_index_cache_prewarm_pause
TABLE=vs_prewarm_pool_metrics
LOG="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.log"

function metrics()
{
    ${CLICKHOUSE_CLIENT} --query "
        SELECT metric, value
        FROM system.metrics
        WHERE metric IN (
            'MergeTreePrewarmVectorSimilarityIndexCacheThreads',
            'MergeTreePrewarmVectorSimilarityIndexCacheThreadsActive',
            'MergeTreePrewarmVectorSimilarityIndexCacheThreadsScheduled')
        ORDER BY metric
        FORMAT TSV"
}

function cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FP}" >/dev/null 2>&1 || true
    wait || true
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE} SYNC" >/dev/null 2>&1 || true
    rm -f "${LOG}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    SYSTEM DROP VECTOR SIMILARITY INDEX CACHE;
    DROP TABLE IF EXISTS ${TABLE};
    CREATE TABLE ${TABLE}
    (
        id UInt64,
        vec Array(Float32),
        INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 1
    )
    ENGINE = MergeTree
    ORDER BY id
    SETTINGS index_granularity = 1;
    INSERT INTO ${TABLE} VALUES (0, [0.0, 0.0]);
    SYSTEM ENABLE FAILPOINT ${FP};
"

${CLICKHOUSE_CLIENT} --max_threads 1 --query "SYSTEM PREWARM VECTOR SIMILARITY INDEX CACHE FOR TABLE ${TABLE}" >"${LOG}" 2>&1 &

if ! timeout 60 ${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${FP} PAUSE"
then
    echo "Prewarm did not reach the failpoint"
    cat "${LOG}" >&2
    exit 1
fi

metrics

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FP}"
wait

metrics
