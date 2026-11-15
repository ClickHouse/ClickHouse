#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `s3_disk` shares the endpoint of `s3_conn`, so both statements copy natively with a clone of the disk client
# that takes the backup retry settings; a jitter factor outside [0, 1] must be clamped there as well.

$CLICKHOUSE_CLIENT -m -q "
    CREATE TABLE data (key Int) ENGINE = MergeTree ORDER BY tuple() SETTINGS disk = 's3_disk';
    INSERT INTO data SELECT * FROM numbers(10);
"

backup="S3(s3_conn, 'backups/$CLICKHOUSE_DATABASE/$(random_str 10)')"
backup_query_id=$(random_str 10)
restore_query_id=$(random_str 10)

$CLICKHOUSE_CLIENT --query_id "$backup_query_id" -q "BACKUP TABLE data TO $backup SETTINGS backup_restore_s3_retry_jitter_factor = 2" | cut -f2
$CLICKHOUSE_CLIENT --query_id "$restore_query_id" -q "RESTORE TABLE data AS data_restored FROM $backup SETTINGS backup_restore_s3_retry_jitter_factor = -1" | cut -f2

$CLICKHOUSE_CLIENT -m -q "
    SELECT count(), sum(key) FROM data_restored;
    SYSTEM FLUSH LOGS query_log;
    SELECT query_kind, ProfileEvents['S3CopyObject'] > 0 FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id IN ('$backup_query_id', '$restore_query_id')
    ORDER BY event_time_microseconds;
"
