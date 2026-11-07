#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/126287
# A failed Iceberg metadata write is retried as a lost commit race only when a file holds the target version afterwards.
# gzip rejects compression level 18 before anything is written, so every commit below must fail with the codec's
# error instead of being retried up to the limit and reported as something else.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

ICEBERG_DIR="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05508"
rm -rf "${ICEBERG_DIR}" "${ICEBERG_DIR}_p"

${CLICKHOUSE_CLIENT} --query "
    SET allow_insert_into_iceberg = 1, allow_experimental_iceberg_compaction = 1, iceberg_manifest_min_count_to_compact = 0;
    SET iceberg_metadata_compression_method = 'gzip';
    CREATE TABLE t_05508 (k Int64) ENGINE = IcebergLocal('${ICEBERG_DIR}', 'Parquet');
    CREATE TABLE t_05508_p (k Int64, p Int64) ENGINE = IcebergLocal('${ICEBERG_DIR}_p', 'Parquet') PARTITION BY p;
    INSERT INTO t_05508 VALUES (1);
    INSERT INTO t_05508 VALUES (2);
    INSERT INTO t_05508 VALUES (3);
    INSERT INTO t_05508_p VALUES (1, 1);
    OPTIMIZE TABLE t_05508 MANIFEST SETTINGS output_format_compression_level = 18; -- { serverError ZLIB_DEFLATE_FAILED }
    INSERT INTO t_05508 SETTINGS output_format_compression_level = 18 VALUES (4); -- { serverError ZLIB_DEFLATE_FAILED }
    ALTER TABLE t_05508 DELETE WHERE k = 1 SETTINGS output_format_compression_level = 18; -- { serverError ZLIB_DEFLATE_FAILED }
    ALTER TABLE t_05508 UPDATE k = 10 WHERE k = 2 SETTINGS output_format_compression_level = 18; -- { serverError ZLIB_DEFLATE_FAILED }
    SET output_format_compression_level = 18;
    ALTER TABLE t_05508 ADD COLUMN c Nullable(Int64); -- { serverError ZLIB_DEFLATE_FAILED }
    ALTER TABLE t_05508_p DROP PARTITION 1; -- { serverError ZLIB_DEFLATE_FAILED }
    SELECT count(), sum(k) FROM t_05508;
    DROP TABLE t_05508;
    DROP TABLE t_05508_p;
"

rm -rf "${ICEBERG_DIR}" "${ICEBERG_DIR}_p"
