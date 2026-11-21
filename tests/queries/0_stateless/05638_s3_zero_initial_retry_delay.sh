#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A zero initial retry delay means retrying without backoff, the S3 client must accept it.

backup="S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/$(random_str 10)')"

$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE data (key Int) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO data SELECT * FROM numbers(10);
"
$CLICKHOUSE_CLIENT -q "BACKUP TABLE data TO $backup SETTINGS backup_restore_s3_retry_initial_backoff_ms = 0" | cut -f2
$CLICKHOUSE_CLIENT -q "RESTORE TABLE data AS data_restored FROM $backup SETTINGS backup_restore_s3_retry_initial_backoff_ms = 0" | cut -f2

$CLICKHOUSE_CLIENT -m -q "
    SELECT count(), sum(key) FROM data_restored;
    CREATE TABLE data_disk (key Int) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS disk = disk(type = 's3', endpoint = 'http://localhost:11111/test/$CLICKHOUSE_DATABASE/$(random_str 10)/',
        access_key_id = 'clickhouse', secret_access_key = 'clickhouse', s3_retry_initial_delay_ms = 0);
    INSERT INTO data_disk SELECT * FROM numbers(10);
    SELECT count(), sum(key) FROM data_disk;
"
