#!/usr/bin/env bash
# Tags: no-parallel, no-replicated-database
# no-parallel: the `EXCEPT TABLES` list of the `BACKUP DATABASE system` queries is computed in
#              advance, so a system log table created by a concurrent test in between would have
#              its data copied into the backup.
# no-replicated-database: named collections are server-global, not database-scoped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

collection="nc_${CLICKHOUSE_DATABASE}"
backup_user="backup_user_05446_${CLICKHOUSE_DATABASE}"

# The system log tables are the only system tables whose data would be copied by
# `BACKUP DATABASE system`, so they are excluded to keep the backups small.
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
CREATE NAMED COLLECTION ${collection} AS key1 = 'nc_secret_value_05446';
DROP USER IF EXISTS ${backup_user};
CREATE USER ${backup_user};
GRANT BACKUP ON *.* TO ${backup_user};
GRANT WRITE ON DISK TO ${backup_user};
"

backup_path() { echo "${CLICKHOUSE_DISKS_FILES}/backups/${CLICKHOUSE_TEST_UNIQUE_NAME}_$1"; }
run_backup() { # <suffix> <query> [<settings>]
    local suffix="$1" query="$2" settings="${3:-}"
    ${CLICKHOUSE_CLIENT} --user "${backup_user}" --query \
        "${query} TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_${suffix}')${settings:+ SETTINGS ${settings}}"
}
check_backup_contents() { # <suffix>: the table's definition is kept, no entry is exported
    test -f "$(backup_path "$1")/metadata/system/named_collections.sql" && echo "the table definition is in the backup"
    find "$(backup_path "$1")/data/system/named_collections" -type f 2>/dev/null | wc -l
}

echo "an explicit selection excluding the table's data requires no rights to read the values"
run_backup explicit_nodata "
    BACKUP TABLE system.named_collections EXCEPT DATA FROM TABLE system.named_collections
" | grep -c "BACKUP_CREATED"
check_backup_contents explicit_nodata

echo "the same selection with the data keeps requiring the rights"
run_backup explicit_denied "BACKUP TABLE system.named_collections" 2>&1 | grep -m1 -c "ACCESS_DENIED"

echo "a wildcard selection with backup_named_collections = 1 excluding the table's data requires no rights"
run_backup wildcard_nodata "
    BACKUP DATABASE system ${except_tables_clause} EXCEPT DATA FROM TABLES system.named_collections
" "backup_named_collections = 1" | grep -c "BACKUP_CREATED"
check_backup_contents wildcard_nodata

echo "the same selection with the data keeps requiring the rights"
run_backup wildcard_denied "
    BACKUP DATABASE system ${except_tables_clause}
" "backup_named_collections = 1" 2>&1 | grep -m1 -c "ACCESS_DENIED"

# The wildcard element asks for the table's data, so the export runs, and the explicit element asks
# only for the table's definition, so it must not turn the implicit-selection default (skip the
# export with a warning for a user without the rights) into a hard requirement of the rights.
echo "a metadata-only explicit selection combined with a wildcard keeps the implicit-selection skip"
output=$(run_backup mixed "
    BACKUP TABLE system.named_collections EXCEPT DATA FROM TABLE system.named_collections,
    DATABASE system ${except_tables_clause}
" 2>&1)
echo "${output}" | grep -c "BACKUP_CREATED"
echo "${output}" | grep -m1 -c "Not exporting the named collections"
check_backup_contents mixed

${CLICKHOUSE_CLIENT} -m --query "
DROP USER ${backup_user};
DROP NAMED COLLECTION ${collection};
"
