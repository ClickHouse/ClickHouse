#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires Azure Blob Storage
# The latest metadata file cached under `iceberg_metadata_staleness_ms` belongs to one storage, one path and,
# with `iceberg_metadata_table_uuid`, one uuid.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

URL='http://localhost:10000/devstoreaccount1'
KEY='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=='
DIR="${CLICKHOUSE_TEST_UNIQUE_NAME}"
X='cont'
Y='cont05620'

${CLICKHOUSE_CLIENT} -m -q "
    SET allow_insert_into_iceberg = 1;
    CREATE TABLE t_missing_x (c0 Int32) ENGINE = IcebergAzure('${URL}', '${X}', '${DIR}/missing/', 'devstoreaccount1', '${KEY}');
    CREATE TABLE t_missing_y (c0 Int32) ENGINE = IcebergAzure('${URL}', '${Y}', '${DIR}/missing/', 'devstoreaccount1', '${KEY}');
    INSERT INTO t_missing_x VALUES (1), (2), (3);
    CREATE TABLE t_older_x (c0 Int32) ENGINE = IcebergAzure('${URL}', '${X}', '${DIR}/older/', 'devstoreaccount1', '${KEY}');
    CREATE TABLE t_older_y (c0 Int32) ENGINE = IcebergAzure('${URL}', '${Y}', '${DIR}/older/', 'devstoreaccount1', '${KEY}');
    INSERT INTO t_older_x VALUES (1), (2), (3);
    INSERT INTO t_older_y VALUES (10);
    INSERT INTO t_older_y VALUES (20);
"

read_count()
{
    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=1 --iceberg_metadata_staleness_ms="$1" \
        --query "SELECT count() FROM icebergAzure('${URL}', '$2', '${DIR}/$3/', 'devstoreaccount1', '${KEY}')"
}

# The same path in two containers: a stale-tolerant read of Y must not take the file just cached for X.
read_count 0 "${Y}" missing
read_count 0 "${X}" missing
read_count 600000 "${Y}" missing
read_count 0 "${Y}" older
read_count 0 "${X}" older
read_count 600000 "${Y}" older

# Two tables in one directory, told apart by `iceberg_metadata_table_uuid`.
for uuid in 'ea8d1178-7756-4b89-b21f-00e9f31fe03e' 'A90EED4CF74B4E5BB630096FB9D09021'; do
    ${CLICKHOUSE_CLIENT} --use_iceberg_metadata_files_cache=1 --iceberg_metadata_staleness_ms=600000 \
        --query "SELECT * FROM icebergS3(s3_conn, filename = 'merged_several_tables_test', SETTINGS iceberg_metadata_table_uuid = '${uuid}') ORDER BY id"
done

${CLICKHOUSE_CLIENT} -m -q "
    DROP TABLE t_missing_x;
    DROP TABLE t_missing_y;
    DROP TABLE t_older_x;
    DROP TABLE t_older_y;
"
