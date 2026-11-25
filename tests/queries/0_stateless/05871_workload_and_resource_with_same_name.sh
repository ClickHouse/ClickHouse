#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# - no-parallel: workloads and resources are global server-wide state, and the backup covers all of them
# - no-fasttest: BACKUP/RESTORE is not available in fasttest builds

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

function show()
{
    $CLICKHOUSE_CLIENT -q "SELECT 'workload', name, create_query FROM system.workloads WHERE name = '05871_x'"
    $CLICKHOUSE_CLIENT -q "SELECT 'resource', name, create_query FROM system.resources WHERE name = '05871_x'"
}

$CLICKHOUSE_CLIENT -m -q "
    DROP WORKLOAD IF EXISTS 05871_x;
    DROP RESOURCE IF EXISTS 05871_x;
    CREATE RESOURCE 05871_x (WRITE DISK 05871_disk, READ DISK 05871_disk);
    CREATE WORKLOAD 05871_x SETTINGS max_io_requests = 10 FOR 05871_x;
"

echo '--- created ---'
show

echo '--- statements address one kind only ---'
$CLICKHOUSE_CLIENT -q "CREATE RESOURCE 05871_x (READ DISK 05871_disk)" 2>&1 | grep -o -m1 "already exists"
$CLICKHOUSE_CLIENT -q "DROP RESOURCE 05871_x" 2>&1 | grep -o -m1 "cannot be dropped"

echo '--- backup ---'
$CLICKHOUSE_CLIENT -q "BACKUP TABLE system.workloads, TABLE system.resources TO ${backup_name}" | cut -f2

$CLICKHOUSE_CLIENT -m -q "
    DROP WORKLOAD 05871_x;
    DROP RESOURCE 05871_x;
"

echo '--- after drop ---'
show

echo '--- restore ---'
$CLICKHOUSE_CLIENT -q "RESTORE TABLE system.workloads, TABLE system.resources FROM ${backup_name}" | cut -f2

echo '--- after restore ---'
show

$CLICKHOUSE_CLIENT -m -q "
    DROP WORKLOAD 05871_x;
    DROP RESOURCE 05871_x;
"
