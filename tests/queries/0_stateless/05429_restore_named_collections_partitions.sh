#!/usr/bin/env bash
# Tags: no-replicated-database
# no-replicated-database: named collections are server-global, not database-scoped.
# No no-parallel: the backup is only read back by a restore which must fail before it creates or
# replaces anything, so the test has no server-global side effects.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

collection="nc_${CLICKHOUSE_DATABASE}"
backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"

${CLICKHOUSE_CLIENT} -m --query "
DROP NAMED COLLECTION IF EXISTS ${collection};
CREATE NAMED COLLECTION ${collection} AS key1 = 'original';
"

echo "backup"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.named_collections TO ${backup_name}" | grep -c "BACKUP_CREATED"

# The restore must reject the PARTITIONS clause before anything is restored: `key2`, added after
# the backup was taken, must survive even with create_named_collection = 'replace'.
echo "a restore with a PARTITIONS clause is rejected before anything is restored"
${CLICKHOUSE_CLIENT} --query "ALTER NAMED COLLECTION ${collection} SET key2 = 'added_after_backup'"
${CLICKHOUSE_CLIENT} --query "
    RESTORE TABLE system.named_collections PARTITIONS '0' FROM ${backup_name}
    SETTINGS create_named_collection = 'replace'
" 2>&1 | grep -m1 -c "Cannot restore the \`system.named_collections\` table with a PARTITIONS clause"
${CLICKHOUSE_CLIENT} --query "SELECT mapContains(collection, 'key2') FROM system.named_collections WHERE name = '${collection}'"

${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION ${collection}"
