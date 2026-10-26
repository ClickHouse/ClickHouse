#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# no-fasttest: the encrypted storage for named collections requires SSL, so
#              tests/config/config.d/named_collections_storage.xml is not installed there.
# no-parallel: `BACKUP TABLE system.named_collections` captures every SQL-created collection on the
#              server, and the restore in 'replace' mode would overwrite collections of concurrent
#              tests.
# no-replicated-database: named collections are server-global, not database-scoped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

collection_a="nc_${CLICKHOUSE_DATABASE}_a"
collection_b="nc_${CLICKHOUSE_DATABASE}_b"
backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_restore"
failpoint="named_collections_restore_pause_after_write"
restore_output="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_restore.out"

${CLICKHOUSE_CLIENT} -m --query "
DROP NAMED COLLECTION IF EXISTS ${collection_a};
DROP NAMED COLLECTION IF EXISTS ${collection_b};
CREATE NAMED COLLECTION ${collection_a} AS key_backup = '1';
CREATE NAMED COLLECTION ${collection_b} AS key_backup = '1';
"

echo "backup"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.named_collections TO ${backup_name}" | grep -c "BACKUP_CREATED"

# The restore in 'replace' mode must find both collections changed. The two versions differ in
# their key names, not in their values: the keys of a collection are visible without the rights to
# read the values, so a stopped write is observable as the kept current key.
${CLICKHOUSE_CLIENT} -m --query "
DROP NAMED COLLECTION ${collection_a};
DROP NAMED COLLECTION ${collection_b};
CREATE NAMED COLLECTION ${collection_a} AS key_current = '1';
CREATE NAMED COLLECTION ${collection_b} AS key_current = '1';
"

# The failpoint holds the restore right after each entry write, so the KILL below arrives while the
# restore is inside a write. The entries are restored in the order of their file names, so the
# first written entry is ${collection_a}.
echo "the restore is cancelled while it is inside an entry write"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${failpoint}"
${CLICKHOUSE_CLIENT} --query_id "${query_id}" --query "
    RESTORE TABLE system.named_collections FROM ${backup_name} SETTINGS create_named_collection = 'replace'
" > "${restore_output}" 2>&1 &
restore_pid=$!

keys=""
for _ in {1..600}; do
    keys=$(${CLICKHOUSE_CLIENT} --query "SELECT mapKeys(collection) FROM system.named_collections WHERE name = '${collection_a}'")
    [ "${keys}" == "['key_backup']" ] && break
    sleep 0.1
done
[ "${keys}" == "['key_backup']" ] || echo "the restore did not write the first entry"

${CLICKHOUSE_CLIENT} --format Null --query "KILL QUERY WHERE query_id = '${query_id}' ASYNC"
# The cancellation must be set before the failpoint releases the restore.
cancelled=""
for _ in {1..600}; do
    cancelled=$(${CLICKHOUSE_CLIENT} --query "SELECT is_cancelled FROM system.processes WHERE query_id = '${query_id}'")
    [ "${cancelled}" == "1" ] && break
    sleep 0.1
done
[ "${cancelled}" == "1" ] || echo "the restore was not marked as cancelled"

${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${failpoint}"
wait "${restore_pid}" || true
grep -m1 -c "QUERY_WAS_CANCELLED" "${restore_output}"

echo "the entry written before the cancellation is restored, the remaining one is unchanged"
${CLICKHOUSE_CLIENT} --query "SELECT mapKeys(collection) FROM system.named_collections WHERE name = '${collection_a}'"
${CLICKHOUSE_CLIENT} --query "SELECT mapKeys(collection) FROM system.named_collections WHERE name = '${collection_b}'"

${CLICKHOUSE_CLIENT} -m --query "
DROP NAMED COLLECTION ${collection_a};
DROP NAMED COLLECTION ${collection_b};
"
