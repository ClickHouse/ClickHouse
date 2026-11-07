#!/usr/bin/env bash
# Tags: no-fasttest

# A Hudi partition metafile written in the base file format
# (`hoodie.partition.metafile.use.base.format=true`) is not a data file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="05567_hudi_partition_metafile/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION s3(s3_conn, filename='${TABLE_PATH}/0000_1-0-1_20260101000000000.parquet')
    SETTINGS s3_truncate_on_insert = 1
    SELECT number AS id, number * 10 AS v FROM numbers(3)
"

${CLICKHOUSE_CLIENT} -q "SELECT id, v FROM hudi(s3_conn, filename='${TABLE_PATH}/') ORDER BY id"

# The metafile, and a leftover temporary one from a failed writer.
for name in .hoodie_partition_metadata.parquet .hoodie_partition_metadata_7e41b120-8dbb-4f03-a487-fab888fe1037.parquet
do
    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO FUNCTION s3(s3_conn, filename='${TABLE_PATH}/${name}')
        SETTINGS s3_truncate_on_insert = 1
        SELECT '20260101000000000' AS commitTime, '0' AS partitionDepth
    "
done

${CLICKHOUSE_CLIENT} -q "SELECT id, v FROM hudi(s3_conn, filename='${TABLE_PATH}/') ORDER BY id"
