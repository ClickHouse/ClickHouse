#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: user-defined types live in a single process-wide namespace, and the backup of
# `system.user_defined_types` contains all of them.
# Tag no-fasttest: uses the `backups` disk.

# `RESTORE` validates all restored types before storing any of them, and if storing one of them fails, the types it
# has already stored are reverted: the server is not left with a part of the backup.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

X="RollbackX_${CLICKHOUSE_DATABASE}"
B="RollbackB_${CLICKHOUSE_DATABASE}"
BACKUP="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

function show_types()
{
    $CLICKHOUSE_CLIENT --query "SELECT name, create_query FROM system.user_defined_types WHERE name IN ('${X}', '${B}') ORDER BY name" \
        | sed "s/${CLICKHOUSE_DATABASE}/DB/g"
}

$CLICKHOUSE_CLIENT --query "DROP TYPE IF EXISTS ${B}"
$CLICKHOUSE_CLIENT --query "DROP TYPE IF EXISTS ${X}"
$CLICKHOUSE_CLIENT --query "CREATE TYPE ${X} AS UInt8"
$CLICKHOUSE_CLIENT --query "CREATE TYPE ${B} AS Array(${X})"
$CLICKHOUSE_CLIENT --query "BACKUP TABLE system.user_defined_types TO ${BACKUP}" | cut -f2

$CLICKHOUSE_CLIENT --query "DROP TYPE ${B}"
$CLICKHOUSE_CLIENT --query "DROP TYPE ${X}"
$CLICKHOUSE_CLIENT --query "CREATE TYPE ${B} AS String"
echo "--- before"
show_types

# `X` is restored first (`B` is defined through it), then storing `B` fails because it already exists, so `X` is removed again.
$CLICKHOUSE_CLIENT --query "RESTORE TABLE system.user_defined_types FROM ${BACKUP} SETTINGS create_function = 'create'" 2>&1 \
    | grep -o -m1 "TYPE_ALREADY_EXISTS"
echo "--- after the failed restore"
show_types
$CLICKHOUSE_CLIENT --query "SELECT toTypeName(CAST('a', '${B}'))"

$CLICKHOUSE_CLIENT --query "DROP TYPE ${B}"
