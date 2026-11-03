#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3 (MinIO) and Iceberg writes.

# `write_full_path_in_iceberg_metadata` is enabled by default: an Iceberg table created on S3 without touching
# the setting records an `s3://bucket/...` location, which is what external readers (PyIceberg, Spark, Trino)
# need to open the table, and reads back its own writes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}"
TABLE="t_iceberg_s3_full_path_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -m -q "
    SET allow_insert_into_iceberg = 1;
    SET iceberg_metadata_compression_method = '';
    SET allow_experimental_iceberg_compaction = 0;
    DROP TABLE IF EXISTS ${TABLE};
    CREATE TABLE ${TABLE} (x Int32, s String) ENGINE = IcebergS3(s3_conn, filename = '${DIR}/');
    INSERT INTO ${TABLE} VALUES (1, 'a'), (2, 'b');
    SELECT * FROM ${TABLE} ORDER BY x;
    DROP TABLE ${TABLE};
"

${CLICKHOUSE_CLIENT} -m -q "
    SELECT _file, replaceOne(JSONExtractString(c, 'location'), '${DIR}', '<dir>')
    FROM s3(s3_conn, filename = '${DIR}/metadata/*.metadata.json', format = 'RawBLOB', structure = 'c String')
    ORDER BY _file;
    SELECT count(), sum(x) FROM icebergS3(s3_conn, filename = '${DIR}/');
"
