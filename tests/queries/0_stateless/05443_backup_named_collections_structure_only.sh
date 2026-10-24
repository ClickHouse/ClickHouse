#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# no-fasttest: the encrypted storage for named collections requires SSL, so
#              tests/config/config.d/named_collections_storage.xml is not installed there.
# no-parallel: `BACKUP TABLE system.named_collections` captures every SQL-created collection on the
#              server and RESTORE re-creates them, which would interfere with concurrent tests.
# no-replicated-database: named collections are server-global, not database-scoped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

collection="nc_${CLICKHOUSE_DATABASE}"
backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
backup_path="${CLICKHOUSE_DISKS_FILES}/backups/${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
DROP NAMED COLLECTION IF EXISTS ${collection};
CREATE NAMED COLLECTION ${collection} AS
    url = 'http://localhost:8123/?query=SELECT+1&password=nc_secret_value' NOT OVERRIDABLE,
    format = 'CSVWithNames' OVERRIDABLE;

CREATE TABLE nc_url_table (n UInt32) ENGINE = URL(${collection});
CREATE TABLE nc_data_table (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO nc_data_table SELECT number FROM numbers(1000);
"

echo "a structure_only backup keeps the collections: a restored object may resolve them in its CREATE"
${CLICKHOUSE_CLIENT} --query "
    BACKUP TABLE system.named_collections, ALL EXCEPT DATABASES system TO ${backup_name}
    SETTINGS structure_only = true
" | grep -c "BACKUP_CREATED"
find "${backup_path}/data/system/named_collections" -type f | grep -c "${collection}"

echo "restore after dropping everything recreates the collection and the table depending on it"
${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE nc_url_table;
DROP TABLE nc_data_table;
DROP NAMED COLLECTION ${collection};
"
${CLICKHOUSE_CLIENT} --query "RESTORE ALL FROM ${backup_name}" | grep -c "RESTORED"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.named_collections WHERE name = '${collection}'
"
${CLICKHOUSE_CLIENT} --query "
    SELECT create_table_query LIKE 'CREATE TABLE%URL(${collection})%'
    FROM system.tables WHERE database = currentDatabase() AND name = 'nc_url_table'
"
echo "the backup held no table data"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM nc_data_table"

echo "backup_named_collections = 0 still turns the export off under structure_only"
${CLICKHOUSE_CLIENT} --query "
    BACKUP TABLE system.named_collections TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_off')
    SETTINGS structure_only = true, backup_named_collections = 0
" | grep -c "BACKUP_CREATED"
find "${CLICKHOUSE_DISKS_FILES}/backups/${CLICKHOUSE_TEST_UNIQUE_NAME}_off/data/system/named_collections" -type f 2>/dev/null | wc -l

echo "EXCEPT DATA FROM TABLE still turns the export off under structure_only"
${CLICKHOUSE_CLIENT} --query "
    BACKUP TABLE system.named_collections EXCEPT DATA FROM TABLE system.named_collections
    TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_except_data')
    SETTINGS structure_only = true
" | grep -c "BACKUP_CREATED"
find "${CLICKHOUSE_DISKS_FILES}/backups/${CLICKHOUSE_TEST_UNIQUE_NAME}_except_data/data/system/named_collections" -type f 2>/dev/null | wc -l

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE nc_url_table;
DROP TABLE nc_data_table;
DROP NAMED COLLECTION ${collection};
"
