#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: user-defined types live in a single process-wide namespace, and the backup of
# `system.user_defined_types` contains all of them.
# Tag no-fasttest: uses the `backups` disk.

# A table can refer to a user-defined type by name in a persisted expression (`ORDER BY CAST(x, 'T')`), so `RESTORE`
# of a backup with both the table and `system.user_defined_types` recreates the types before the tables.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

KEY_TYPE="BackupKey_${CLICKHOUSE_DATABASE}"
BACKUP="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS bt"
$CLICKHOUSE_CLIENT --query "DROP TYPE IF EXISTS ${KEY_TYPE}"
$CLICKHOUSE_CLIENT --query "CREATE TYPE ${KEY_TYPE} AS UInt64"
$CLICKHOUSE_CLIENT --query "CREATE TABLE bt (x UInt8) ENGINE = MergeTree ORDER BY CAST(x, '${KEY_TYPE}')"
$CLICKHOUSE_CLIENT --query "INSERT INTO bt VALUES (2), (10)"

$CLICKHOUSE_CLIENT --query "BACKUP TABLE ${CLICKHOUSE_DATABASE}.bt, TABLE system.user_defined_types TO ${BACKUP}" | cut -f2

$CLICKHOUSE_CLIENT --query "DROP TABLE bt SYNC"
$CLICKHOUSE_CLIENT --query "DROP TYPE ${KEY_TYPE}"

$CLICKHOUSE_CLIENT --query "RESTORE TABLE ${CLICKHOUSE_DATABASE}.bt, TABLE system.user_defined_types FROM ${BACKUP}" | cut -f2
$CLICKHOUSE_CLIENT --query "SELECT create_query FROM system.user_defined_types WHERE name = '${KEY_TYPE}'" | sed "s/${CLICKHOUSE_DATABASE}/DB/g"
$CLICKHOUSE_CLIENT --query "SELECT * FROM bt WHERE CAST(x, '${KEY_TYPE}') = 10"
$CLICKHOUSE_CLIENT --query "SELECT * FROM bt ORDER BY x"

$CLICKHOUSE_CLIENT --query "DROP TABLE bt SYNC"
$CLICKHOUSE_CLIENT --query "DROP TYPE ${KEY_TYPE}"
