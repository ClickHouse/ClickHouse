#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# no-parallel: backups selecting `system.named_collections` capture every SQL-created collection
#              on the server, which would interfere with concurrent tests.
# no-replicated-database: named collections are server-global, not database-scoped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

collection="nc_${CLICKHOUSE_DATABASE}"
user="user_05428_${CLICKHOUSE_DATABASE}"

# `BACKUP DATABASE system` selects `system.named_collections` the same implicit way `BACKUP ALL`
# does, without the cost of copying the data of every database. The system log tables are the only
# system tables whose data would be copied, so they are excluded to keep the backup small.
log_tables=$(${CLICKHOUSE_CLIENT} --query "
    SELECT arrayStringConcat(groupArray(name), ', ') FROM system.tables
    WHERE database = 'system' AND engine LIKE '%MergeTree'
")
except_tables_clause=""
if [ -n "${log_tables}" ]; then
    except_tables_clause="EXCEPT TABLES ${log_tables}"
fi

${CLICKHOUSE_CLIENT} -m --query "
DROP NAMED COLLECTION IF EXISTS ${collection};
CREATE NAMED COLLECTION ${collection} AS key1 = 'nc_secret_value_05428';
DROP USER IF EXISTS ${user};
CREATE USER ${user};
GRANT BACKUP ON *.* TO ${user};
GRANT WRITE ON DISK TO ${user};
"

backup_path() { echo "${CLICKHOUSE_DISKS_FILES}/backups/${CLICKHOUSE_TEST_UNIQUE_NAME}_$1"; }
run_backup() { # <suffix> <user> <query> [<settings>]
    local suffix="$1" as_user="$2" query="$3" settings="${4:-}"
    ${CLICKHOUSE_CLIENT} --user "${as_user}" --query \
        "${query} TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_${suffix}')${settings:+ SETTINGS ${settings}}"
}

echo "unset: an implicit selection skips the collections for a user without the rights to read them"
# The skip emits a server-side warning. Request it through `send_logs_level` and capture it, both
# to assert it and to keep it out of the test's own stderr, which must stay empty.
output=$(${CLICKHOUSE_CLIENT} --send_logs_level=warning --user "${user}" --query \
    "BACKUP DATABASE system ${except_tables_clause} TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_default_skip')" 2>&1)
echo "${output}" | grep -c "BACKUP_CREATED"
echo "${output}" | grep -m1 -c "Not exporting the named collections"
find "$(backup_path default_skip)/data/system/named_collections" -type f 2>/dev/null | wc -l

echo "unset: an implicit selection exports them for a user holding the rights"
run_backup default_export default "BACKUP DATABASE system ${except_tables_clause}" | grep -c "BACKUP_CREATED"
find "$(backup_path default_export)/data/system/named_collections" -type f | grep -c "${collection}"

echo "backup_named_collections = 1 makes the missing rights an error"
run_backup denied "${user}" "BACKUP DATABASE system ${except_tables_clause}" "backup_named_collections = 1" 2>&1 | grep -m1 -c "ACCESS_DENIED"

echo "backup_named_collections = 0 never exports, even explicitly selected, and needs no rights"
run_backup off "${user}" "BACKUP TABLE system.named_collections" "backup_named_collections = 0" | grep -c "BACKUP_CREATED"
find "$(backup_path off)/data/system/named_collections" -type f 2>/dev/null | wc -l

echo "an explicit selection requires the rights"
run_backup explicit_denied "${user}" "BACKUP TABLE system.named_collections" 2>&1 | grep -m1 -c "ACCESS_DENIED"

${CLICKHOUSE_CLIENT} -m --query "
DROP USER ${user};
DROP NAMED COLLECTION ${collection};
"
