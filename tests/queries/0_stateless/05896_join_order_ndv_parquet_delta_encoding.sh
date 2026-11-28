#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs S3

# The compressed size of a delta-encoded Parquet column does not tell its number of distinct values: the 100000
# unique sorted keys of `05896_delta_binary_packed_unique_keys.parquet` (one row group, `DELTA_BINARY_PACKED`, ZSTD)
# take 522 bytes, which would be estimated as a single value and the self-join as 10 billion rows. Such a column
# gets no estimate from its size, as a `MergeTree` column with `CODEC(Delta)`. The file is written by pyarrow,
# because ClickHouse does not write the delta encodings.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"

$CLICKHOUSE_CLIENT --query "INSERT INTO FUNCTION s3(s3_conn, filename = '${FILE}', format = 'RawBLOB') SETTINGS s3_truncate_on_insert = 1 FORMAT RawBLOB" < "$CUR_DIR/data_parquet/05896_delta_binary_packed_unique_keys.parquet"

SETTINGS="SET use_statistics = 1, use_parquet_metadata_statistics = 1, estimate_distinct_values_from_metadata = 1,
    query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0, query_plan_join_swap_table = 'auto',
    enable_join_runtime_filters = 0, enable_parallel_replicas = 0;"

$CLICKHOUSE_CLIENT --query "
$SETTINGS
CREATE TABLE t (k Int64) ENGINE = S3(s3_conn, filename = '${FILE}', format = 'Parquet');

SELECT count(), uniqExact(k) FROM t;

SELECT 'The rows of the file';
SELECT extract(arrayStringConcat(groupArray(explain), '\n'), 'Left: rows estimated ([^\n]+)')
FROM (EXPLAIN SELECT count() FROM t AS a JOIN t AS b ON a.k = b.k);

SELECT 'No single distinct value from the size of a delta-encoded column, so the join is not a cross product';
SELECT toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\\\(NDV\\\\) ([0-9.e+-]+)')) < 0.01
FROM (EXPLAIN SELECT count() FROM t AS a JOIN t AS b ON a.k = b.k);

DROP TABLE t;
"
