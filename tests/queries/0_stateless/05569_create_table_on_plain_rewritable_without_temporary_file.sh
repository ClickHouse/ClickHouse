#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: requires S3
# Tag no-replicated-database: plain rewritable should not be shared between replicas

# `CREATE TABLE` in a database whose metadata is on a `plain_rewritable` disk over S3 writes the metadata file of
# the table in place: no temporary file that is copied to the final key and deleted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function plain_rewritable_disk()
{
    local name=$1
    echo "disk(
        name = '${name}',
        type = s3_plain_rewritable,
        endpoint = 'http://localhost:11111/test/05569/${name}/',
        access_key_id = clickhouse,
        secret_access_key = clickhouse)"
}

function blob_events()
{
    local disk_name=$1
    local since=$2

    $CLICKHOUSE_CLIENT -m -q "
        SYSTEM FLUSH LOGS blob_storage_log;
        SELECT
            countIf(event_type = 'Copy') AS copies,
            countIf(event_type = 'Delete') AS deletes,
            countIf(remote_path LIKE '%.sql.tmp%') AS temporary,
            countIf(event_type = 'Upload' AND endsWith(remote_path, '/t.sql')) AS uploads
        FROM system.blob_storage_log
        WHERE event_date >= yesterday() AND disk_name = '${disk_name}' AND toUnixTimestamp64Micro(event_time_microseconds) >= ${since};
    "
}

function check()
{
    local db=$1
    local disk_definition=$2
    local disk_name=$3

    $CLICKHOUSE_CLIENT -m -q "
        DROP DATABASE IF EXISTS ${db} SYNC;
        CREATE DATABASE ${db} ENGINE = Atomic SETTINGS disk = ${disk_definition};
    "

    local create_time
    # A number, not local time text: the text is ambiguous in the hour repeated when DST ends.
    create_time=$($CLICKHOUSE_CLIENT -q "SELECT toUnixTimestamp64Micro(now64(6))")
    $CLICKHOUSE_CLIENT -q "CREATE TABLE ${db}.t (x UInt64) ENGINE = MergeTree ORDER BY x"
    blob_events "$disk_name" "$create_time"

    # The table is loaded back from its metadata file.
    local db_uuid
    db_uuid=$($CLICKHOUSE_CLIENT -q "SELECT uuid FROM system.databases WHERE name = '${db}'")
    $CLICKHOUSE_CLIENT -m -q "
        DETACH DATABASE ${db};
        ATTACH DATABASE ${db} UUID '${db_uuid}' ENGINE = Atomic SETTINGS disk = ${disk_definition};
        SELECT name, type FROM system.columns WHERE database = '${db}' AND table = 't';
        INSERT INTO ${db}.t VALUES (42);
        SELECT * FROM ${db}.t;
    "
}

plain_disk="05569_plain_${CLICKHOUSE_DATABASE}"
plain_db="${CLICKHOUSE_DATABASE}_plain"
check "$plain_db" "$(plain_rewritable_disk "$plain_disk")" "$plain_disk"

encrypted_inner_disk="05569_encrypted_inner_${CLICKHOUSE_DATABASE}"
encrypted_db="${CLICKHOUSE_DATABASE}_encrypted"
check "$encrypted_db" "disk(
    type = encrypted,
    disk = disk(
        type = cache,
        max_size = '16Mi',
        max_file_segment_size = '4Mi',
        path = '05569_cache_${CLICKHOUSE_DATABASE}/',
        disk = $(plain_rewritable_disk "$encrypted_inner_disk")),
    key = '1234567812345678')" "$encrypted_inner_disk"

$CLICKHOUSE_CLIENT -m -q "
    DROP DATABASE ${plain_db} SYNC;
    DROP DATABASE ${encrypted_db} SYNC;
"
