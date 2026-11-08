#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test, depends on S3
# Tag no-msan: delta-kernel-rs is not built with MSan

# Delta Lake tables written by Spark with deletion vectors, a v2 checkpoint with sidecars, and the change data feed,
# read from S3 with `deltaLake`, `deltaLakeCluster`, and the `DeltaLake` table engine: the deletion vector files and
# the sidecars are read from object storage. The corpus tables (see `05560_delta_lake_corpus`) are uploaded file by file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PREFIX="${CLICKHOUSE_TEST_UNIQUE_NAME}"
STAGE="${CLICKHOUSE_USER_FILES_UNIQUE}_s3"
rm -rf "$STAGE"
mkdir -p "$STAGE"

for table in deletion_vectors v2_checkpoint_parquet change_data_feed_deletion_vectors
do
    cp -r "$CUR_DIR/data_lake_corpus/delta/$table" "$STAGE/$table"
    (cd "$STAGE" && find "$table" -type f) | while read -r file
    do
        ${CLICKHOUSE_CLIENT} --query "INSERT INTO FUNCTION s3(s3_conn, filename = '$PREFIX/$file', format = 'RawBLOB')
            SELECT * FROM file('$STAGE/$file', 'RawBLOB')"
    done
done

${CLICKHOUSE_CLIENT} --session_timezone UTC --query "
SELECT '-- deletion vectors';
SELECT count(), sum(id), countIf(s = 'updated') FROM deltaLake(s3_conn, filename = '$PREFIX/deletion_vectors');
SELECT count(), sum(id) FROM deltaLake(s3_conn, filename = '$PREFIX/deletion_vectors') SETTINGS optimize_count_from_files = 0;
SELECT count(), sum(id) FROM deltaLake(s3_conn, filename = '$PREFIX/deletion_vectors') WHERE id % 2 = 0;
SELECT count(), sum(id) FROM deltaLake(s3_conn, filename = '$PREFIX/deletion_vectors') SETTINGS delta_lake_snapshot_version = 3;
SELECT count(), sum(id) FROM deltaLakeCluster('test_cluster_two_shards_localhost', s3_conn, filename = '$PREFIX/deletion_vectors');
SELECT id, _row_number FROM deltaLake(s3_conn, filename = '$PREFIX/deletion_vectors') WHERE id IN (1, 101) ORDER BY id;

SELECT '-- v2 checkpoint with sidecars';
SELECT count(), sum(id) FROM deltaLake(s3_conn, filename = '$PREFIX/v2_checkpoint_parquet');
SELECT count(), sum(id) FROM deltaLakeCluster('test_cluster_two_shards_localhost', s3_conn, filename = '$PREFIX/v2_checkpoint_parquet');

SELECT '-- change data feed';
SELECT _commit_version, _change_type, id, s FROM deltaLake(s3_conn, filename = '$PREFIX/change_data_feed_deletion_vectors')
    WHERE _commit_version >= 2 ORDER BY _commit_version, id, _change_type SETTINGS delta_lake_snapshot_start_version = 2;

SELECT '-- table engine';
DROP TABLE IF EXISTS dv_s3;
CREATE TABLE dv_s3 ENGINE = DeltaLake(s3_conn, filename = '$PREFIX/deletion_vectors');
SELECT count(), sum(id) FROM dv_s3;
SELECT count(), sum(id) FROM dv_s3 SETTINGS delta_lake_snapshot_version = 1;
DROP TABLE dv_s3;
"

rm -rf "$STAGE"
