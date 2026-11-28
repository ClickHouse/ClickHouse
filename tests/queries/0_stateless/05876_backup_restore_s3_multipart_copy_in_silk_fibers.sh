#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3

# With `backup_restore_s3_use_silk` the native multipart copies of BACKUP and RESTORE run in Silk fibers.
# One part in flight makes the parent fiber wait for a finished part before it starts the next one.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS data;
    DROP TABLE IF EXISTS data_restored;
    CREATE TABLE data (key UInt64, s String) ENGINE = MergeTree ORDER BY key
        SETTINGS disk = 's3_disk', min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;
    INSERT INTO data SELECT number, randomString(1024) FROM numbers(8192);
    OPTIMIZE TABLE data FINAL;
"

prefix="backups/$CLICKHOUSE_DATABASE"
# The ~8 MiB column file exceeds the threshold, so it is copied with two UploadPartCopy requests.
silk_copy_settings=(
    --backup_restore_s3_use_silk 1
    --s3_max_single_operation_copy_size 5242880
    --s3_min_upload_part_size 5242880
    --s3_max_inflight_parts_for_one_file 1
)

$CLICKHOUSE_CLIENT --format Null "${silk_copy_settings[@]}" --query_id "${CLICKHOUSE_DATABASE}_backup" \
    -q "BACKUP TABLE data TO S3(s3_conn, '$prefix/fibers') SETTINGS allow_s3_native_copy = 1"
$CLICKHOUSE_CLIENT --format Null "${silk_copy_settings[@]}" --query_id "${CLICKHOUSE_DATABASE}_restore" \
    -q "RESTORE TABLE data AS data_restored FROM S3(s3_conn, '$prefix/fibers') SETTINGS allow_s3_native_copy = 1"

$CLICKHOUSE_CLIENT -m -q "
    SELECT
        (SELECT count() FROM data_restored),
        (SELECT groupBitXor(cityHash64(key, s)) FROM data_restored) = (SELECT groupBitXor(cityHash64(key, s)) FROM data);

    SYSTEM FLUSH LOGS blob_storage_log, query_log;

    -- The copies ran in fibers, not in the backups IO thread pool.
    SELECT
        sumIf(ProfileEvents['S3MultipartCopyInFibers'], query_id = '${CLICKHOUSE_DATABASE}_backup') > 0,
        sumIf(ProfileEvents['S3MultipartCopyInFibers'], query_id = '${CLICKHOUSE_DATABASE}_restore') > 0
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
        AND type = 'QueryFinish' AND query_id IN ('${CLICKHOUSE_DATABASE}_backup', '${CLICKHOUSE_DATABASE}_restore');

    -- The backup and the restore each copy at least one file with a complete multipart copy of two or more parts.
    SELECT
        countIf(query_id = '${CLICKHOUSE_DATABASE}_backup') > 0,
        countIf(query_id = '${CLICKHOUSE_DATABASE}_restore') > 0
    FROM
    (
        SELECT query_id, remote_path
        FROM system.blob_storage_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND query_id IN ('${CLICKHOUSE_DATABASE}_backup', '${CLICKHOUSE_DATABASE}_restore')
        GROUP BY query_id, remote_path
        HAVING countIf(event_type = 'MultiPartUploadCreate' AND error_code = 0) = 1
            AND countIf(event_type = 'MultiPartUploadComplete' AND error_code = 0) = 1
            AND countIf(event_type = 'MultiPartUploadAbort') = 0
            AND countIf(event_type = 'Copy' AND error_code = 0) >= 2
            AND maxIf(data_size, event_type = 'Copy' AND error_code = 0) <= 5242880
    );

    DROP TABLE data;
    DROP TABLE data_restored;
"
