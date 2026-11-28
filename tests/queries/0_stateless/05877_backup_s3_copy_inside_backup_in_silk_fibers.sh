#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
# Tag no-fasttest: requires S3

# With `backup_restore_s3_use_silk` a copy inside a backup (`BackupWriterS3::copyFile`) runs its native multipart copy in
# Silk fibers. A plain backup (`deduplicate_files = 0`) writes the `data.bin` of the second KeeperMap table on the same
# Keeper path as a copy of the first one, and that copy is the only native copy of this backup.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

keeper_path="/${CLICKHOUSE_DATABASE}/05877"
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS kv1;
    DROP TABLE IF EXISTS kv2;
    CREATE TABLE kv1 (key UInt64, value String) ENGINE = KeeperMap('$keeper_path') PRIMARY KEY key;
    CREATE TABLE kv2 (key UInt64, value String) ENGINE = KeeperMap('$keeper_path') PRIMARY KEY key;
    -- ~8 MiB of incompressible values, more than one part of 5 MiB.
    INSERT INTO kv1 SELECT number, randomString(8192) FROM numbers(1024);
"

prefix="backups/$CLICKHOUSE_DATABASE"
$CLICKHOUSE_CLIENT --format Null --query_id "${CLICKHOUSE_DATABASE}_backup" \
    --backup_restore_s3_use_silk 1 --s3_max_single_operation_copy_size 5242880 --s3_min_upload_part_size 5242880 \
    -q "BACKUP TABLE kv1, TABLE kv2 TO S3(s3_conn, '$prefix/plain') SETTINGS deduplicate_files = 0, allow_s3_native_copy = 1"

# One `data.bin` is written from Keeper, the other one is its copy: they are equal byte by byte.
$CLICKHOUSE_CLIENT -q "
    SELECT count(), uniqExact(sipHash64(raw)), min(length(raw)) > 5242880
    FROM s3(s3_conn, filename = '$prefix/plain/data/${CLICKHOUSE_DATABASE}/{kv1,kv2}/data.bin', format = 'RawBLOB', structure = 'raw String')"

expected=$($CLICKHOUSE_CLIENT -q "SELECT count(), sum(cityHash64(key, value)) FROM kv1")
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE kv1 SYNC;
    DROP TABLE kv2 SYNC;
"
$CLICKHOUSE_CLIENT --format Null -q "RESTORE TABLE kv1, TABLE kv2 FROM S3(s3_conn, '$prefix/plain')"
[[ "$($CLICKHOUSE_CLIENT -q "SELECT count(), sum(cityHash64(key, value)) FROM kv2")" == "$expected" ]] && echo "restored"

$CLICKHOUSE_CLIENT -m -q "
    SYSTEM FLUSH LOGS blob_storage_log, query_log;

    SELECT ProfileEvents['S3MultipartCopyInFibers'] > 0
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase()
        AND type = 'QueryFinish' AND query_id = '${CLICKHOUSE_DATABASE}_backup';

    -- The copy of data.bin is a complete multipart copy of two parts.
    SELECT count() FROM
    (
        SELECT remote_path
        FROM system.blob_storage_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND query_id = '${CLICKHOUSE_DATABASE}_backup' AND remote_path LIKE '%/data.bin'
        GROUP BY remote_path
        HAVING countIf(event_type = 'MultiPartUploadCreate' AND error_code = 0) = 1
            AND countIf(event_type = 'MultiPartUploadComplete' AND error_code = 0) = 1
            AND countIf(event_type = 'Copy' AND error_code = 0) = 2
    );

    DROP TABLE kv1 SYNC;
    DROP TABLE kv2 SYNC;
"
