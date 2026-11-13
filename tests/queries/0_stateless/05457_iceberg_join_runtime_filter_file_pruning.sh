#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_rf_pruning"
rm -rf "${ROOT}"
trap 'rm -rf "${ROOT}"' EXIT

# 16 data files, file i holding k = i * 100 .. i * 100 + 99, so a key of the join runtime filter
# falls into the bounds of one file only. `nk` is NULL in two files, which min/max pruning never skips.
# Background Iceberg compaction would rewrite the files.
inserts=""
for i in $(seq 0 15); do
    inserts="${inserts}
INSERT INTO fact SELECT number + $((i * 100)) AS k, if(k IN (10, 710), NULL, k), k, toInt32(k) FROM numbers(100);"
done

${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --max_insert_threads=1 --query "
CREATE TABLE fact (k Int64, nk Nullable(Int64), v Int64, k32 Int32) ENGINE = IcebergLocal('${ROOT}/fact/', 'Parquet')
SETTINGS allow_experimental_iceberg_compaction = 0;
${inserts}
CREATE TABLE dim (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO dim VALUES (705), (710);
CREATE TABLE dim64 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO dim64 VALUES (5);
" < /dev/null

# Every setting that decides whether the probe side gets a runtime filter, or which side is probed, is pinned.
SETTINGS="enable_join_runtime_filters = 1, join_algorithm = 'hash', enable_join_runtime_filters_index_analysis = 1, use_iceberg_partition_pruning = 1,
    join_runtime_filter_min_probe_rows = 0, query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0,
    use_hash_table_stats_for_join_reordering = 0, enable_parallel_replicas = 0, use_iceberg_metadata_files_cache = 0"

# Runs the query with the pruning on and off: both must return the same rows.
probe() { # probe <tag> <query> [<extra settings>] [<max_threads>]
    local extra="${3:+, $3}, max_threads = ${4:-1}"
    ${CLICKHOUSE_CLIENT} --query "
        $2 SETTINGS ${SETTINGS}${extra}, use_join_runtime_filters_for_iceberg_pruning = 1, log_comment = '${CLICKHOUSE_DATABASE}_$1_on';
        $2 SETTINGS ${SETTINGS}${extra}, use_join_runtime_filters_for_iceberg_pruning = 0, log_comment = '${CLICKHOUSE_DATABASE}_$1_off';"
}

# The number of data files the probe pruned, with the pruning on and off; with a second argument `> 0`, whether it pruned any.
report() { # report <tag> [> 0]
    ${CLICKHOUSE_CLIENT} --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT replaceOne(log_comment, '${CLICKHOUSE_DATABASE}_', ''), ProfileEvents['IcebergRuntimeFilterPrunedFiles'] ${2:-}
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
            AND log_comment IN ('${CLICKHOUSE_DATABASE}_$1_on', '${CLICKHOUSE_DATABASE}_$1_off')
        ORDER BY log_comment DESC
        SETTINGS enable_parallel_replicas = 0"
}

echo '--- A an exact set of keys ---'
probe a "SELECT count(), sum(v) FROM fact AS f INNER JOIN dim AS d ON f.k = d.k"
report a

echo '--- A2 the same through the iterator wrapper of a static filter ---'
probe a2 "SELECT count(), sum(v) FROM fact AS f INNER JOIN dim AS d ON f.k = d.k WHERE f.v >= 0"
report a2

echo '--- A4 the same with 4 threads ---'
probe a4 "SELECT count(), sum(v) FROM fact AS f INNER JOIN dim AS d ON f.k = d.k" "" 4
report a4 "> 0"

echo '--- B the [min, max] range when the exact set is too large ---'
probe b "SELECT count(), sum(v) FROM fact AS f INNER JOIN dim AS d ON f.k = d.k" "join_runtime_filter_exact_values_limit = 1"
report b

echo '--- C a key under two casts prunes nothing ---'
probe c "SELECT count(), sum(v) FROM fact AS f INNER JOIN dim64 AS d ON CAST(f.k AS UInt8) = d.k"
report c

echo '--- D a Nullable key ---'
probe d "SELECT count(), sum(v) FROM fact AS f INNER JOIN dim AS d ON f.nk = d.k"
report d

echo '--- E a key under one cast ---'
probe e "SELECT count(), sum(v) FROM fact AS f INNER JOIN dim AS d ON f.k32 = d.k"
report e

echo '--- H LEFT ANTI prunes nothing ---'
probe h "SELECT count() FROM fact AS f LEFT ANTI JOIN dim AS d ON f.k = d.k"
report h

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS fact SYNC;
    DROP TABLE IF EXISTS dim SYNC;
    DROP TABLE IF EXISTS dim64 SYNC;"
