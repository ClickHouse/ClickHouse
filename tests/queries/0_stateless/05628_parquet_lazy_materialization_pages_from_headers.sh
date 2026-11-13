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

# Data pages of 1 KiB in row groups of 30000 rows; the array column has repeated values, so its chunks are read whole.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${PREFIX}.parquet')
    SELECT number AS id, intDiv(number, 10) AS t, number * 7919 % 100003 AS k, repeat('x', number % 50) AS s,
        if(number % 7 = 0, NULL, number % 1000) AS n, toLowCardinality(toString(number % 23)) AS lc, [number, number + 1] AS arr
    FROM numbers(100000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_write_page_index = 0, output_format_parquet_data_page_size = 1024,
        output_format_parquet_row_group_size = 30000, output_format_parquet_compression_method = 'zstd'"

$CLICKHOUSE_CLIENT -q "SELECT count() > 0 FROM (EXPLAIN SELECT * FROM file('${PREFIX}.parquet') ORDER BY k LIMIT 5 SETTINGS ${LAZY})
    WHERE explain ILIKE '%LazilyReadFromFile%'"

$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${PREFIX}.parquet') ORDER BY t DESC, id LIMIT 3 SETTINGS ${LAZY}, log_comment = '${PREFIX}_lazy'"
$CLICKHOUSE_CLIENT -q "SELECT * FROM file('${PREFIX}.parquet') WHERE id % 1000 = 17 ORDER BY k DESC, id LIMIT 3 OFFSET 2
    SETTINGS ${LAZY}, log_comment = '${PREFIX}_lazy'"
for settings in "${LAZY}, log_comment = '${PREFIX}_lazy'" "${EAGER}, log_comment = '${PREFIX}_eager'"; do
    $CLICKHOUSE_CLIENT -q "SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT * FROM file('${PREFIX}.parquet') ORDER BY k, id LIMIT 3000)
        SETTINGS ${settings}"
done

# Layouts ClickHouse does not write: data pages V2, and a dictionary page at `data_page_offset` (DuckDB 0.9).
for f in arrow-go-v18.4.1/flat.v2.6.dpv2 arrow-go-v18.4.1/flat.dict_fallback.dpv2 pyarrow-26.0.0/flat.zstd22.dpv2 duckdb-0.9.2/flat.zstd; do
    cp "${CORPUS}/${f}.parquet" "${USER_FILES_PATH}/${PREFIX}_corpus.parquet"
    for settings in "${LAZY}, log_comment = '${PREFIX}_lazy'" "${EAGER}, log_comment = '${PREFIX}_eager'"; do
        $CLICKHOUSE_CLIENT -q "SELECT '${f}', count(), sum(cityHash64(toString(tuple(*)))) FROM (SELECT * FROM file('${PREFIX}_corpus.parquet') ORDER BY id DESC LIMIT 3)
            SETTINGS ${settings}"
    done
done

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"
$CLICKHOUSE_CLIENT -q "
    SELECT replaceOne(log_comment, '${PREFIX}_', ''), countIf(ProfileEvents['ParquetColumnChunksPagesLocatedFromHeaders'] > 0), count()
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('${PREFIX}_lazy', '${PREFIX}_eager')
    GROUP BY log_comment ORDER BY log_comment"

rm -f "${USER_FILES_PATH}/${PREFIX}.parquet" "${USER_FILES_PATH}/${PREFIX}_corpus.parquet"
