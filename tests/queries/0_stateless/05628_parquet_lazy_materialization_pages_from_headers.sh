#!/usr/bin/env bash
# Random settings limits: query_plan_max_limit_for_lazy_materialization=(10000, None); use_columns_cache=(0, 0)

# Lazy materialization reads the deferred columns only for the rows that survive the `LIMIT`. In a local Parquet file
# without an offset index, the pages holding these rows are located from the page headers, and only these pages are
# read. The results must be the same as without lazy materialization.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CORPUS="${CUR_DIR}/data_lake_corpus/parquet"
PREFIX="${CLICKHOUSE_TEST_UNIQUE_NAME}"
LAZY="query_plan_optimize_lazy_materialization = 1, query_plan_optimize_lazy_materialization_for_file = 1"
EAGER="query_plan_optimize_lazy_materialization = 0"

# The same rows with 32 KiB pages (located from the headers) and with 1 KiB pages (chunks read whole); the array column
# has repeated values, so its chunks are always read whole.
# The row groups follow the INSERT's block size, so the fixture INSERTs pin it.
for pages in 32768 1024; do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION file('${PREFIX}_${pages}.parquet')
        SELECT number AS id, intDiv(number, 10) AS t, number * 7919 % 100003 AS k, repeat('x', number % 50) AS s,
            if(number % 7 = 0, NULL, number % 1000) AS n, toLowCardinality(toString(number % 23)) AS lc, [number, number + 1] AS arr
        FROM numbers(100000)
        SETTINGS max_block_size = 65409, engine_file_truncate_on_insert = 1, output_format_parquet_write_page_index = 0, output_format_parquet_data_page_size = ${pages},
            output_format_parquet_row_group_size = 30000, output_format_parquet_compression_method = 'none'"
done

# Without dictionaries the deferred columns are most of the bytes, so reading only the located pages reads far less.
# With 1 MiB pages every chunk has one data page, which is not located.
for pages in 32768 1024 1048576; do
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION file('${PREFIX}_bytes_${pages}.parquet')
        SELECT toUInt32(number) AS id, number * 3 AS a, number * 5 AS b, number * 7 AS c, number * 11 AS d, number * 13 AS e,
            number * 17 AS f
        FROM numbers(100000)
        SETTINGS max_block_size = 65409, engine_file_truncate_on_insert = 1, output_format_parquet_write_page_index = 0, output_format_parquet_data_page_size = ${pages},
            output_format_parquet_row_group_size = 30000, output_format_parquet_compression_method = 'none',
            output_format_parquet_max_dictionary_size = 0"
done

$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM (EXPLAIN SELECT * FROM file('${PREFIX}_32768.parquet') ORDER BY k LIMIT 5 SETTINGS ${LAZY})
    WHERE explain ILIKE '%LazilyReadFromFile%'"

for pages in 32768 1024; do
    $CLICKHOUSE_CLIENT -q "SELECT * FROM file('${PREFIX}_${pages}.parquet') ORDER BY t DESC, id LIMIT 3
        SETTINGS ${LAZY}, log_comment = '${PREFIX}_lazy_${pages}'"
    $CLICKHOUSE_CLIENT -q "SELECT * FROM file('${PREFIX}_${pages}.parquet') WHERE id % 1000 = 17 ORDER BY k DESC, id LIMIT 3 OFFSET 2
        SETTINGS ${LAZY}, log_comment = '${PREFIX}_lazy_${pages}'"
    for settings in "${LAZY}, log_comment = '${PREFIX}_lazy_${pages}'" "${EAGER}, log_comment = '${PREFIX}_eager'"; do
        $CLICKHOUSE_CLIENT -q "SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT * FROM file('${PREFIX}_${pages}.parquet') ORDER BY k, id LIMIT 3000)
            SETTINGS ${settings}"
    done
done

# Layouts ClickHouse does not write: data pages V2, and a dictionary page at `data_page_offset` (DuckDB 0.9).
for f in arrow-go-v18.4.1/flat.v2.6.dpv2 arrow-go-v18.4.1/flat.dict_fallback.dpv2 pyarrow-26.0.0/flat.zstd22.dpv2 duckdb-0.9.2/flat.zstd; do
    cp "${CORPUS}/${f}.parquet" "${USER_FILES_PATH}/${PREFIX}_corpus.parquet"
    for settings in "${LAZY}, log_comment = '${PREFIX}_lazy_corpus'" "${EAGER}, log_comment = '${PREFIX}_eager'"; do
        $CLICKHOUSE_CLIENT -q "SELECT '${f}', count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT * FROM file('${PREFIX}_corpus.parquet') ORDER BY id DESC LIMIT 3)
            SETTINGS ${settings}"
    done
done

for pages in 32768 1024 1048576; do
    $CLICKHOUSE_CLIENT -q "SELECT * FROM file('${PREFIX}_bytes_${pages}.parquet') WHERE id IN (5, 45005, 95005) ORDER BY id LIMIT 3 FORMAT Null
        SETTINGS ${LAZY}, log_comment = '${PREFIX}_bytes_${pages}'"
done

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT sumIf(ProfileEvents['ParquetPrefetcherReadBytes'], log_comment = '${PREFIX}_bytes_32768') * 2
        < sumIf(ProfileEvents['ParquetPrefetcherReadBytes'], log_comment = '${PREFIX}_bytes_1024')
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('${PREFIX}_bytes_32768', '${PREFIX}_bytes_1024')"
$CLICKHOUSE_CLIENT -q "
    SELECT replaceOne(log_comment, '${PREFIX}_', ''), countIf(ProfileEvents['ParquetColumnChunksPagesLocatedFromHeaders'] > 0), count()
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('${PREFIX}_lazy_32768',
        '${PREFIX}_lazy_1024', '${PREFIX}_lazy_corpus', '${PREFIX}_eager', '${PREFIX}_bytes_32768', '${PREFIX}_bytes_1024', '${PREFIX}_bytes_1048576')
    GROUP BY log_comment ORDER BY log_comment"

rm -f "${USER_FILES_PATH}/${PREFIX}_32768.parquet" "${USER_FILES_PATH}/${PREFIX}_1024.parquet" \
    "${USER_FILES_PATH}/${PREFIX}_bytes_32768.parquet" "${USER_FILES_PATH}/${PREFIX}_bytes_1024.parquet" "${USER_FILES_PATH}/${PREFIX}_bytes_1048576.parquet" \
    "${USER_FILES_PATH}/${PREFIX}_corpus.parquet"
