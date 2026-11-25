#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A restored part with non-adaptive granularity takes its granule size from the table's index_granularity, so it is
# restored only into a table with the index_granularity it was backed up with.
backup="Disk('backups', '${CLICKHOUSE_DATABASE}/05877_backup')"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS t_restore_fixed;
    DROP TABLE IF EXISTS t_restore;
    DROP TABLE IF EXISTS t_restore_same;

    CREATE TABLE t_restore_fixed (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
    CREATE TABLE t_restore (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;

    INSERT INTO t_restore_fixed SELECT number FROM numbers(20000);
    ALTER TABLE t_restore ATTACH PARTITION ID 'all' FROM t_restore_fixed;
"

$CLICKHOUSE_CLIENT -q "BACKUP TABLE t_restore TO $backup FORMAT Null"

# The same index_granularity: the part is restored.
$CLICKHOUSE_CLIENT -m -q "
    RESTORE TABLE t_restore AS t_restore_same FROM $backup FORMAT Null;
    SELECT 'restored', count(), sum(a) FROM t_restore_same;
"

# A changed index_granularity: the part is rejected.
$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE t_restore SYNC;
    CREATE TABLE t_restore (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760;
    ALTER TABLE t_restore MODIFY SETTING index_granularity = 1024;
    RESTORE TABLE t_restore FROM $backup SETTINGS allow_different_table_def = 1 FORMAT Null; -- { serverError CANNOT_RESTORE_TABLE }
    SELECT 'rejected', count() FROM t_restore;
"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE t_restore;
    DROP TABLE t_restore_same;
    DROP TABLE t_restore_fixed;
"
