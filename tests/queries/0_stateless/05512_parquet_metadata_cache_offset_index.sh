#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# Tag no-fasttest: needs MinIO
# Tag no-parallel-replicas: profile events are not available on the other replicas
# Random settings limits: use_columns_cache=(0, 0); use_query_condition_cache=(0, 0)

# A read that finds the footer of a Parquet file in the metadata cache keeps the offset indexes it reads with it,
# so that later reads of the file do not request them again. Every file is private to the test database, so the
# cache is not dropped. A column served from the columns cache, or a read skipped by the query condition cache,
# reads no offset index. A `clickhouse-local` process checks that the kept offset indexes count in the cache
# weight, and that a local file whose version token has not settled neither keeps nor uses them.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
SET log_queries = 1;
SET use_parquet_metadata_cache = 1;
SET input_format_parquet_use_offset_index = 1;

-- One row group, several pages per column.
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SELECT number AS a, number % 97 AS b, number AS z FROM numbers(100000)
SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 100000, output_format_parquet_data_page_size = 65536;

-- The miss inserts the footer, the first hit keeps the offset indexes of the three columns, later reads use them.
SELECT sum(a), sum(b), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:miss';
SELECT sum(a), sum(b), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:hit_keeps';
SELECT sum(a), sum(b), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:hit_kept';
SELECT count(), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
WHERE b = 3 SETTINGS log_comment = '05512:filter';
-- The column index of \`a\` selects pages through its kept offset index.
SELECT count(), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
WHERE a BETWEEN 30000 AND 30999 SETTINGS input_format_parquet_page_filter_push_down = 1, log_comment = '05512:page_filter';

-- Rewritten in place with two row groups: a new version, whose offset indexes are read and kept again.
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SELECT number AS a, number % 97 AS b, number AS z FROM numbers(100000)
SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 50000, output_format_parquet_data_page_size = 65536;

SELECT sum(a), sum(b), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:rewritten_miss';
SELECT sum(a), sum(b), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:rewritten_hit_keeps';
SELECT sum(a), sum(b), sum(z) FROM s3(s3_conn, filename = currentDatabase() || '_05512.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:rewritten_hit_kept';

-- An offset index of about 1500 pages is larger than what the cache keeps for this small footer: it is never kept.
INSERT INTO FUNCTION s3(s3_conn, filename = currentDatabase() || '_05512_pages.parquet', format = 'Parquet')
SELECT number AS a FROM numbers(200000)
SETTINGS s3_truncate_on_insert = 1, output_format_parquet_data_page_size = 1024, output_format_parquet_batch_size = 64;

SELECT sum(a) FROM s3(s3_conn, filename = currentDatabase() || '_05512_pages.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:large_miss';
SELECT sum(a) FROM s3(s3_conn, filename = currentDatabase() || '_05512_pages.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:large_hit_keeps';
SELECT sum(a) FROM s3(s3_conn, filename = currentDatabase() || '_05512_pages.parquet', format = 'Parquet')
SETTINGS log_comment = '05512:large_hit_kept';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['ParquetMetadataCacheHits'] AS hits,
    ProfileEvents['ParquetMetadataCacheMisses'] AS misses,
    ProfileEvents['ParquetMetadataCacheOffsetIndexHits'] AS offset_index_hits
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase() AND log_comment LIKE '05512:%'
ORDER BY event_time_microseconds;
"

DIR="${CLICKHOUSE_DATABASE}_05512"
DATA_DIR="${USER_FILES_PATH:?}/${DIR}"
rm -rf "${DATA_DIR:?}"
mkdir -p "$DATA_DIR"
${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION file('${DIR}/a.parquet', Parquet, 'a UInt64, b UInt64, z UInt64')
    SELECT number, number % 97, number FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 100000, output_format_parquet_data_page_size = 65536,
        engine_file_truncate_on_insert = 1"
cp "$DATA_DIR/a.parquet" "$DATA_DIR/a_unsettled.parquet"
touch -d '2020-01-01 00:00:00' "$DATA_DIR/a.parquet"
touch -d '2099-01-01 00:00:00' "$DATA_DIR/a_unsettled.parquet"

S="use_parquet_metadata_cache = 1, use_columns_cache = 0, use_query_condition_cache = 0"
Q_WEIGHT="SELECT value FROM system.metrics WHERE metric = 'ParquetMetadataCacheBytes';"
Q_HITS="SELECT ifNull((SELECT value FROM system.events WHERE event = 'ParquetMetadataCacheOffsetIndexHits'), 0);"
read_file() # file, input_format_parquet_use_offset_index
{
    echo "SELECT sum(a), sum(b), sum(z) FROM file('${DATA_DIR}/$1.parquet', Parquet, 'a UInt64, b UInt64, z UInt64')
        SETTINGS $S, input_format_parquet_use_offset_index = $2 FORMAT Null;"
}
reads() # file
{
    echo "$(read_file "$1" 0) $(read_file "$1" 0) $Q_WEIGHT $Q_HITS $(read_file "$1" 1) $Q_WEIGHT
        $(read_file "$1" 1) $Q_WEIGHT $Q_HITS"
}
read -r w1 h1 w2 w3 h2 u1 uh1 u2 u3 uh2 <<< "$(${CLICKHOUSE_LOCAL} --query "$(reads a) $(reads a_unsettled)" | tr '\n' ' ')"
echo "clickhouse-local: weight counts the kept offset indexes $((w2 > w1)), unchanged on reuse $((w3 == w2)), offset index hits $((h2 - h1))"
echo "clickhouse-local, unsettled version token: weight unchanged $((u2 == u1)) $((u3 == u2)), offset index hits $((uh2 - uh1))"

rm -rf "${DATA_DIR:?}"
