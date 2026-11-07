#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# Tag no-parallel: user-defined types live in a single process-wide namespace, and the backup of
# `system.user_defined_types` contains all of them.
# Tag no-fasttest: uses the `backups` disk.

# `RESTORE ... SETTINGS create_function = 'replace'` validates the restored types as the set they will become, so a
# type and its dependent can both change their shape: replacing `A` alone would break the current `B`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

A="RestoreA_${CLICKHOUSE_DATABASE}"
B="RestoreB_${CLICKHOUSE_DATABASE}"
BACKUP="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

function show_types()
{
    $CLICKHOUSE_CLIENT --query "SELECT name, create_query FROM system.user_defined_types WHERE name IN ('${A}', '${B}') ORDER BY name" \
        | sed "s/${CLICKHOUSE_DATABASE}/DB/g"
}

$CLICKHOUSE_CLIENT --query "DROP TYPE IF EXISTS ${B}"
$CLICKHOUSE_CLIENT --query "DROP TYPE IF EXISTS ${A}"
$CLICKHOUSE_CLIENT --query "CREATE TYPE ${A}(K, V) AS Map(K, V)"
$CLICKHOUSE_CLIENT --query "CREATE TYPE ${B} AS ${A}(String, UInt8)"
$CLICKHOUSE_CLIENT --query "BACKUP TABLE system.user_defined_types TO ${BACKUP}" | cut -f2

$CLICKHOUSE_CLIENT --query "DROP TYPE ${B}"
$CLICKHOUSE_CLIENT --query "DROP TYPE ${A}"
$CLICKHOUSE_CLIENT --query "CREATE TYPE ${A}(T) AS Array(T)"
$CLICKHOUSE_CLIENT --query "CREATE TYPE ${B} AS ${A}(UInt8)"
echo "--- before"
show_types

# Replacing `A` alone is rejected: `B` uses it with one argument.
$CLICKHOUSE_CLIENT --query "CREATE TYPE OR REPLACE ${A}(K, V) AS Map(K, V)" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

$CLICKHOUSE_CLIENT --query "RESTORE TABLE system.user_defined_types FROM ${BACKUP} SETTINGS create_function = 'replace'" | cut -f2
echo "--- replaced"
show_types
$CLICKHOUSE_CLIENT --query "SELECT toTypeName(CAST(map('a', 1), '${B}'))"

$CLICKHOUSE_CLIENT --query "DROP TYPE ${B}"
$CLICKHOUSE_CLIENT --query "DROP TYPE ${A}"
