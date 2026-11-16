#!/usr/bin/env bash
# Tags: no-fasttest

# A Hudi partition metafile (`.hoodie_partition_metadata.parquet`) is written next to the data files when
# `hoodie.partition.metafile.use.base.format=true`. It has the base file extension but is not a data file,
# so it must not make reading the table fail.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="05567_hudi_partition_metafile/${CLICKHOUSE_TEST_UNIQUE_NAME}"

# A Hudi data file is `[FileId]_[FileWriteToken]_[Timestamp].[ext]`.
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3(s3_conn, filename='${TABLE_PATH}/0000_1-0-1_20260101000000.parquet')
    SETTINGS s3_truncate_on_insert = 1
    SELECT number AS id FROM numbers(3)
"

${CLICKHOUSE_CLIENT} -q "SELECT id FROM hudi(s3_conn, filename='${TABLE_PATH}/') ORDER BY id"

${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3(s3_conn, filename='${TABLE_PATH}/.hoodie_partition_metadata.parquet')
    SETTINGS s3_truncate_on_insert = 1
    SELECT 'partition_depth' AS key, '0' AS value
"

${CLICKHOUSE_CLIENT} -q "SELECT id FROM hudi(s3_conn, filename='${TABLE_PATH}/') ORDER BY id"
