#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: requires the `backups` disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A restored part that misses a column named like a subcolumn of another column (`a.size0` next to an `Array` column `a`)
# reads that name as the subcolumn when a TTL expression reads it. Such a part is refused by `RESTORE`, as by `ATTACH`,
# so the TTL does not expire its rows by values it never held.

BACKUP_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}"

# shellcheck disable=SC2016
$CLICKHOUSE_CLIENT --query "
    DROP TABLE IF EXISTS t;
    CREATE TABLE t (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
    INSERT INTO t VALUES ([]);
    ALTER TABLE t ADD COLUMN \`a.size0\` UInt64 DEFAULT 1;
    SELECT a, \`a.size0\` FROM t;
    BACKUP TABLE t TO Disk('backups', '${BACKUP_NAME}') FORMAT Null;
    TRUNCATE TABLE t;
    ALTER TABLE t MODIFY TTL addDays(toDate('2000-01-01'), 36500 * \`a.size0\`) SETTINGS materialize_ttl_after_modify = 0;
"

$CLICKHOUSE_CLIENT --query "
    RESTORE TABLE t FROM Disk('backups', '${BACKUP_NAME}') SETTINGS allow_different_table_def = 1 FORMAT Null;
" 2>&1 | grep -o -m1 'ALTER_OF_COLUMN_IS_FORBIDDEN'

# shellcheck disable=SC2016
$CLICKHOUSE_CLIENT --query "
    SELECT count() FROM t;

    -- Without the TTL, the part is restored and keeps reading the default.
    ALTER TABLE t REMOVE TTL;
    RESTORE TABLE t FROM Disk('backups', '${BACKUP_NAME}') SETTINGS allow_different_table_def = 1, allow_non_empty_tables = 1 FORMAT Null;
    SELECT a, \`a.size0\` FROM t;
    DROP TABLE t;
"
