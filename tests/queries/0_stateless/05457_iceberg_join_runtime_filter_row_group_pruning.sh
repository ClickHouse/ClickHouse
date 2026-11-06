#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_DATABASE}_rf_row_groups"
rm -rf "${ROOT}"
trap 'rm -rf "${ROOT}"' EXIT

# One data file of 16 row groups, row group i holding k = i * 100 .. i * 100 + 99, so whole-file pruning cannot
# skip anything and a key of the join runtime filter falls into the bounds of one row group only. `nk` is NULL
# for k = 10 and k = 710. The row group size of an Iceberg write is the one of the table. Background Iceberg
# compaction would rewrite the file.
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --max_insert_threads=1 --query "
CREATE TABLE fact (k Int64, nk Nullable(Int64), v Int64, k32 Int32) ENGINE = IcebergLocal('${ROOT}/fact/', 'Parquet')
SETTINGS allow_experimental_iceberg_compaction = 0, output_format_parquet_row_group_size = 100;
INSERT INTO fact SELECT number AS k, if(k IN (10, 710), NULL, k), k, toInt32(k) FROM numbers(1600);
CREATE TABLE dim (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO dim VALUES (705), (710);
CREATE TABLE dim64 (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO dim64 VALUES (5);
" < /dev/null

echo '--- layout: data files, row groups ---'
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(num_row_groups) FROM file('${ROOT}/fact/data/*.parquet', ParquetMetadata)"

# Every setting that decides whether the probe side gets a runtime filter, which side is probed, or whether row
# groups are pruned by statistics is pinned.
SETTINGS="enable_join_runtime_filters = 1, join_algorithm = 'hash', enable_join_runtime_filters_index_analysis = 1, use_iceberg_partition_pruning = 1,
    join_runtime_filter_min_probe_rows = 0, query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0,
    use_hash_table_stats_for_join_reordering = 0, enable_parallel_replicas = 0, use_iceberg_metadata_files_cache = 0,
    input_format_parquet_filter_push_down = 1, use_query_condition_cache = 0"

# Runs the query with the pruning on and off: both must return the same rows.
probe() { # probe <tag> <query> [<extra settings>] [<max_threads>]
    local extra="${3:+, $3}, max_threads = ${4:-1}"
    ${CLICKHOUSE_CLIENT} --query "
        $2 SETTINGS ${SETTINGS}${extra}, use_join_runtime_filters_for_iceberg_pruning = 1, log_comment = '${CLICKHOUSE_DATABASE}_$1_on';
        $2 SETTINGS ${SETTINGS}${extra}, use_join_runtime_filters_for_iceberg_pruning = 0, log_comment = '${CLICKHOUSE_DATABASE}_$1_off';"
}

# The number of row groups the probe pruned by the runtime filter, with the pruning on and off.
report() { # report <tag>
    ${CLICKHOUSE_CLIENT} --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT replaceOne(log_comment, '${CLICKHOUSE_DATABASE}_', ''), ProfileEvents['ParquetRuntimeFilterPrunedRowGroups']
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
            AND log_comment IN ('${CLICKHOUSE_DATABASE}_$1_on', '${CLICKHOUSE_DATABASE}_$1_off')
        ORDER BY log_comment DESC
        SETTINGS enable_parallel_replicas = 0"
}

echo '--- A an exact set of keys ---'
probe a "SELECT count(), sum(v) FROM fact AS f INNER JOIN dim AS d ON f.k = d.k"
report a

echo '--- A4 the same with 4 threads ---'
probe a4 "SELECT count(), sum(v) FROM fact AS f INNER JOIN dim AS d ON f.k = d.k" "" 4
report a4

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

echo '--- P the order of the file is preserved ---'
probe p "SELECT count(), sum(v) FROM fact AS f INNER JOIN dim AS d ON f.k = d.k" "input_format_parquet_preserve_order = 1"
report p

echo '--- W together with a filter of the query ---'
probe w "SELECT count(), sum(v) FROM fact AS f INNER JOIN dim AS d ON f.k = d.k WHERE f.v >= 0"
report w

# The row groups the runtime filter skipped still match `v >= 0`, so the query condition cache must not learn
# from this read: the plain query after it reads the whole table.
echo '--- Q1 the query condition cache ---'
probe q1 "SELECT count(), sum(v) FROM fact AS f INNER JOIN dim AS d ON f.k = d.k WHERE f.v >= 0" \
    "optimize_move_to_prewhere = 0, use_query_condition_cache = 1"
report q1
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM fact WHERE v >= 0 SETTINGS use_query_condition_cache = 1, use_iceberg_metadata_files_cache = 0"

# A schema transform: the old data file reads its columns under the names of its own schema, where `k` is now
# `k_old`, so the runtime filter on the current `k` must not prune its row groups.
echo '--- R a column renamed onto the name of the key ---'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --max_insert_threads=1 --query "
CREATE TABLE fact_r (k Int64, v Int64) ENGINE = IcebergLocal('${ROOT}/fact_r/', 'Parquet')
SETTINGS allow_experimental_iceberg_compaction = 0, output_format_parquet_row_group_size = 100;
INSERT INTO fact_r SELECT number, 1599 - number FROM numbers(1600);
ALTER TABLE fact_r RENAME COLUMN k TO k_old;
ALTER TABLE fact_r RENAME COLUMN v TO k;
INSERT INTO fact_r VALUES (0, 705);
" < /dev/null
probe r "SELECT count(), sum(k_old) FROM fact_r AS f INNER JOIN dim AS d ON f.k = d.k"
report r

# A data file with position deletes is read through a per-file copy of the filters.
echo '--- X position deletes ---'
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --use_iceberg_metadata_files_cache=0 --max_insert_threads=1 --query "
CREATE TABLE fact_x (k Int64, v Int64) ENGINE = IcebergLocal('${ROOT}/fact_x/', 'Parquet')
SETTINGS allow_experimental_iceberg_compaction = 0, output_format_parquet_row_group_size = 100;
INSERT INTO fact_x SELECT number, number FROM numbers(1600);
ALTER TABLE fact_x DELETE WHERE k = 705;
" < /dev/null
probe x "SELECT count(), sum(v) FROM fact_x AS f INNER JOIN dim AS d ON f.k = d.k"
report x

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS fact SYNC;
    DROP TABLE IF EXISTS fact_r SYNC;
    DROP TABLE IF EXISTS fact_x SYNC;
    DROP TABLE IF EXISTS dim SYNC;
    DROP TABLE IF EXISTS dim64 SYNC;"
