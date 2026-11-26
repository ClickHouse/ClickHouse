#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires S3
# Random settings limits: output_format_parquet_write_page_index=(1, 1)

# JOIN runtime filters prune the Parquet row groups and pages of a plain object storage read, but only by a key the
# format returns with the values of the file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# `fact`: 16 row groups, row group i holding k = i * 100 .. i * 100 + 99, so a key of the join runtime filter falls
# into the bounds of one row group only. `fact_p`: 2 row groups of 800 rows and pages of 100 rows. `fact_q2`: a copy
# of `fact` that only the row count cache case reads.
${CLICKHOUSE_CLIENT} --query "
INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/fact.parquet', format = Parquet, structure = 'k Int64, v Int64')
SELECT number, number FROM numbers(1600) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/fact_q2.parquet', format = Parquet, structure = 'k Int64, v Int64')
SELECT number, number FROM numbers(1600) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/fact_p.parquet', format = Parquet, structure = 'k Int64, v Int64')
SELECT number, number FROM numbers(1600) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 800,
    output_format_parquet_batch_size = 100, output_format_parquet_data_page_size = 1;
CREATE TABLE dim (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO dim VALUES (705), (710);
" < /dev/null

FACT="s3(s3_conn, filename = '${DIR}/fact.parquet', format = Parquet, structure = 'k Int64, v Int64')"

echo '--- layout: row groups ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT num_row_groups FROM s3(s3_conn, filename = '${DIR}/fact.parquet', format = ParquetMetadata);
    SELECT num_row_groups FROM s3(s3_conn, filename = '${DIR}/fact_p.parquet', format = ParquetMetadata);"

# Every setting that decides whether the probe side gets a runtime filter, which side is probed, or whether row
# groups and pages are pruned by statistics is pinned.
SETTINGS="enable_join_runtime_filters = 1, join_algorithm = 'hash', enable_join_runtime_filters_index_analysis = 1,
    join_runtime_filter_min_probe_rows = 0, query_plan_join_swap_table = 'false', query_plan_optimize_join_order_randomize = 0,
    use_hash_table_stats_for_join_reordering = 0, enable_parallel_replicas = 0, input_format_parquet_filter_push_down = 1,
    use_query_condition_cache = 0, max_threads = 1"

# Runs the query with the pruning on and off: both must return the same rows.
probe() { # probe <tag> <query> [<extra settings>]
    local extra="${3:+, $3}"
    ${CLICKHOUSE_CLIENT} --query "
        $2 SETTINGS ${SETTINGS}${extra}, use_join_runtime_filters_for_iceberg_pruning = 1, log_comment = '${CLICKHOUSE_DATABASE}_$1_on';
        $2 SETTINGS ${SETTINGS}${extra}, use_join_runtime_filters_for_iceberg_pruning = 0, log_comment = '${CLICKHOUSE_DATABASE}_$1_off';"
}

# The number of row groups and pages the probe pruned by the runtime filter, with the pruning on and off.
report() { # report <tag>
    ${CLICKHOUSE_CLIENT} --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT replaceOne(log_comment, '${CLICKHOUSE_DATABASE}_', ''), ProfileEvents['ParquetRuntimeFilterPrunedRowGroups'],
            ProfileEvents['ParquetRuntimeFilterPrunedPages']
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish'
            AND log_comment IN ('${CLICKHOUSE_DATABASE}_$1_on', '${CLICKHOUSE_DATABASE}_$1_off')
        ORDER BY log_comment DESC
        SETTINGS enable_parallel_replicas = 0"
}

echo '--- A an exact set of keys ---'
probe a "SELECT count(), sum(v) FROM ${FACT} AS f INNER JOIN dim AS d ON f.k = d.k"
report a

echo '--- B the [min, max] range when the exact set is too large ---'
probe b "SELECT count(), sum(v) FROM ${FACT} AS f INNER JOIN dim AS d ON f.k = d.k" "join_runtime_filter_exact_values_limit = 1"
report b

echo '--- P pages ---'
probe p "SELECT count(), sum(v) FROM s3(s3_conn, filename = '${DIR}/fact_p.parquet', format = Parquet, structure = 'k Int64, v Int64') AS f
    INNER JOIN dim AS d ON f.k = d.k" "input_format_parquet_page_filter_push_down = 1"
report p

# The first read inserts the footer into the metadata cache, the next two keep the offset indexes and the column index
# of `k` with it. The fifth finds both there and prunes the same pages.
echo '--- W a read that finds the page index in the metadata cache ---'
${CLICKHOUSE_CLIENT} --query "
INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/fact_w.parquet', format = Parquet, structure = 'k Int64, v Int64')
SELECT number, number FROM numbers(1600) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 800,
    output_format_parquet_batch_size = 100, output_format_parquet_data_page_size = 1;
" < /dev/null
W_QUERY="SELECT count(), sum(v) FROM s3(s3_conn, filename = '${DIR}/fact_w.parquet', format = Parquet, structure = 'k Int64, v Int64') AS f
    INNER JOIN dim AS d ON f.k = d.k"
W_SETTINGS="input_format_parquet_page_filter_push_down = 1, use_parquet_metadata_cache = 1, input_format_parquet_use_offset_index = 1"
probe w1 "$W_QUERY" "$W_SETTINGS"
probe w2 "$W_QUERY" "$W_SETTINGS"
probe w3 "$W_QUERY" "$W_SETTINGS"
report w1
report w3
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['ParquetMetadataCacheOffsetIndexHits'] > 0, ProfileEvents['ParquetMetadataCacheColumnIndexHits'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '${CLICKHOUSE_DATABASE}_w3_on'
    SETTINGS enable_parallel_replicas = 0"

# The second scan keeps the offset index of `k` (100 pages) in the 4 KiB the cell of a small footer gives the offset
# indexes, which leaves no room for its column index there. The column indexes have as much room of their own.
echo '--- X a read after a scan that filled the room of the offset indexes ---'
${CLICKHOUSE_CLIENT} --query "
INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/fact_x.parquet', format = Parquet, structure = 'k Int64')
SELECT number FROM numbers(10000) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 10000,
    output_format_parquet_row_group_size_bytes = 1000000000, output_format_parquet_batch_size = 100, output_format_parquet_data_page_size = 1;
" < /dev/null
X_FILE="s3(s3_conn, filename = '${DIR}/fact_x.parquet', format = Parquet, structure = 'k Int64')"
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(k) FROM ${X_FILE} SETTINGS ${SETTINGS}, ${W_SETTINGS} FORMAT Null;
    SELECT sum(k) FROM ${X_FILE} SETTINGS ${SETTINGS}, ${W_SETTINGS} FORMAT Null;"
probe x1 "SELECT count() FROM ${X_FILE} AS f INNER JOIN dim AS d ON f.k = d.k" "$W_SETTINGS"
probe x2 "SELECT count() FROM ${X_FILE} AS f INNER JOIN dim AS d ON f.k = d.k" "$W_SETTINGS"
report x2
${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['ParquetMetadataCacheOffsetIndexHits'] > 0, ProfileEvents['ParquetMetadataCacheColumnIndexHits'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '${CLICKHOUSE_DATABASE}_x2_on'
    SETTINGS enable_parallel_replicas = 0"

# The offset index of `k` (70 pages) fits into the 4 KiB of a small footer, its column index (64 characters per bound)
# does not. A read that finds the offset index in the cache then does not read that column index for the runtime filter.
echo '--- Y a read after a scan, of a column index the metadata cache cannot keep ---'
${CLICKHOUSE_CLIENT} --query "
INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/fact_y.parquet', format = Parquet, structure = 'k String')
SELECT hex(SHA256(toString(number))) FROM numbers(7000) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 10000,
    output_format_parquet_row_group_size_bytes = 1000000000, output_format_parquet_batch_size = 100, output_format_parquet_data_page_size = 1,
    output_format_parquet_max_dictionary_size = 0;
CREATE TABLE dim_y (k String) ENGINE = MergeTree ORDER BY k;
INSERT INTO dim_y SELECT hex(SHA256(toString(number))) FROM numbers(100, 2);
" < /dev/null
Y_FILE="s3(s3_conn, filename = '${DIR}/fact_y.parquet', format = Parquet, structure = 'k String')"
# A scan served from the columns cache would not read the offset index.
Y_SETTINGS="${W_SETTINGS}, use_columns_cache = 0"
${CLICKHOUSE_CLIENT} --query "
    SELECT sum(length(k)) FROM ${Y_FILE} SETTINGS ${SETTINGS}, ${Y_SETTINGS} FORMAT Null;
    SELECT sum(length(k)) FROM ${Y_FILE} SETTINGS ${SETTINGS}, ${Y_SETTINGS} FORMAT Null;"
probe y "SELECT count() FROM ${Y_FILE} AS f INNER JOIN dim_y AS d ON f.k = d.k" "$Y_SETTINGS"
report y
# The same bytes and reads with the pruning on as off.
${CLICKHOUSE_CLIENT} --query "
    SELECT
        uniqExact(ProfileEvents['ParquetPrefetcherReadBytes']) = 1,
        uniqExact(ProfileEvents['ParquetPrefetcherReadRandomRead'] + ProfileEvents['ParquetPrefetcherReadSeekAndRead']) = 1,
        minIf(ProfileEvents['ParquetMetadataCacheOffsetIndexHits'], log_comment LIKE '%_on') > 0,
        maxIf(ProfileEvents['ParquetMetadataCacheColumnIndexHits'], log_comment LIKE '%_on') = 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('${CLICKHOUSE_DATABASE}_y_on', '${CLICKHOUSE_DATABASE}_y_off')
    SETTINGS enable_parallel_replicas = 0"

# With a structure of only Hive partition columns the format reads `key` from the file (1000 .. 2599), while the
# query takes it from the path (9). Without `optimize_count_from_files = 0` the read only counts the rows of the file.
echo '--- H a Hive partition column read from the file ---'
${CLICKHOUSE_CLIENT} --query "
INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/b=1/key=9/data.parquet', format = Parquet, structure = 'key Int64')
SELECT 1000 + number FROM numbers(1600) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
CREATE TABLE dim_h (k Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO dim_h VALUES (9);
" < /dev/null
probe h "SELECT count() FROM s3(s3_conn, filename = '${DIR}/b=1/key=9/data.parquet', format = Parquet, structure = 'key Int64') AS f
    INNER JOIN dim_h AS d ON f.key = d.k" "use_hive_partitioning = 1, optimize_count_from_files = 0"
report h

# The NULL of `k` in row group 3 (k = 300 .. 399) becomes the default 705 above the format.
echo '--- D a column with a default expression ---'
${CLICKHOUSE_CLIENT} --query "
INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/fact_d.parquet', format = Parquet, structure = 'k Nullable(Int64), v Int64')
SELECT if(number = 310, NULL, number), number FROM numbers(1600) SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
CREATE TABLE fact_d (k Int64 DEFAULT 705, v Int64) ENGINE = S3(s3_conn, filename = '${DIR}/fact_d.parquet', format = Parquet);
" < /dev/null
probe d "SELECT count(), sum(v) FROM fact_d AS f INNER JOIN dim AS d ON f.k = d.k" "input_format_null_as_default = 1"
report d

# Of the runtime filters on `k` and `v`, only the one on `v` prunes row groups: all but 3 and 7.
echo '--- S a runtime filter on a column with a default expression and one on an ordinary column ---'
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE dim_kv (k Int64, v Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO dim_kv VALUES (705, 705), (705, 310), (710, 710);
" < /dev/null
probe s "SELECT count(), sum(f.v) FROM fact_d AS f INNER JOIN dim_kv AS d ON f.k = d.k AND f.v = d.v" "input_format_null_as_default = 1"
report s

# The statistics of a `FIXED_LEN_BYTE_ARRAY` read as `String` keep the trailing zero bytes the cast strips: row group 7
# holds 'ab' with the bounds 'ab\0\0'.
echo '--- F a FixedString column read as String ---'
${CLICKHOUSE_CLIENT} --query "
INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/fact_f.parquet', format = Parquet, structure = 'k FixedString(4), v Int64')
SELECT toFixedString(if(intDiv(number, 100) = 7, 'ab', 'zz'), 4), number FROM numbers(1600)
SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
CREATE TABLE dim_f (k String) ENGINE = MergeTree ORDER BY k;
INSERT INTO dim_f VALUES ('ab');
" < /dev/null
${CLICKHOUSE_CLIENT} --query "SELECT columns.physical_type FROM s3(s3_conn, filename = '${DIR}/fact_f.parquet', format = ParquetMetadata)"
probe f "SELECT count(), sum(v) FROM s3(s3_conn, filename = '${DIR}/fact_f.parquet', format = Parquet, structure = 'k String, v Int64') AS f
    INNER JOIN dim_f AS d ON f.k = d.k"
report f

# The row groups the runtime filter skipped still match `v >= 0`, so the query condition cache must not learn
# from this read: the plain query after it reads the whole file.
echo '--- Q1 the query condition cache ---'
probe q1 "SELECT count(), sum(v) FROM ${FACT} AS f INNER JOIN dim AS d ON f.k = d.k WHERE f.v >= 0" \
    "optimize_move_to_prewhere = 0, use_query_condition_cache = 1"
report q1
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${FACT} WHERE v >= 0 SETTINGS use_query_condition_cache = 1"

# A read that skipped row groups must not record the row count of the file. No `probe` here: its `_off` read would
# write the entry. A cached row count is used only for a file last modified before the second it was cached in.
echo '--- Q2 the per-file row count cache ---'
${CLICKHOUSE_CLIENT} --query "
    SELECT sleep(1) FORMAT Null;
    SELECT count(), sum(v) FROM s3(s3_conn, filename = '${DIR}/fact_q2.parquet', format = Parquet, structure = 'k Int64, v Int64') AS f
    INNER JOIN dim AS d ON f.k = d.k
    SETTINGS ${SETTINGS}, use_join_runtime_filters_for_iceberg_pruning = 1, optimize_move_to_prewhere = 0,
        use_cache_for_count_from_files = 1, log_comment = '${CLICKHOUSE_DATABASE}_q2_on';
    SELECT countIf(number_of_rows IS NOT NULL AND number_of_rows != 1600) FROM system.schema_inference_cache
    WHERE storage = 'S3' AND format = 'Parquet' AND source LIKE '%${DIR}/fact_q2.parquet';
    SELECT count() FROM s3(s3_conn, filename = '${DIR}/fact_q2.parquet', format = Parquet, structure = 'k Int64, v Int64')
    SETTINGS optimize_count_from_files = 1, use_cache_for_count_from_files = 1;"
report q2

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS fact_d SYNC;
    DROP TABLE IF EXISTS dim SYNC;
    DROP TABLE IF EXISTS dim_h SYNC;
    DROP TABLE IF EXISTS dim_kv SYNC;
    DROP TABLE IF EXISTS dim_f SYNC;
    DROP TABLE IF EXISTS dim_y SYNC;"
