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
backup_nc_path="${backup_path}/data/system/named_collections"

${CLICKHOUSE_CLIENT} -m --query "
DROP NAMED COLLECTION IF EXISTS ${collection};
CREATE NAMED COLLECTION ${collection} AS
    url = 'http://localhost:8123/?query=SELECT+1&password=nc_secret_value' NOT OVERRIDABLE,
    format = 'CSVWithNames' OVERRIDABLE;

CREATE TABLE nc_url_table (n UInt32) ENGINE = URL(${collection});
CREATE VIEW nc_url_view AS SELECT * FROM url(${collection}, structure = 'n UInt32');
CREATE TABLE nc_data_table (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO nc_data_table SELECT number FROM numbers(1000);
"

echo "backup"
${CLICKHOUSE_CLIENT} --query "
    BACKUP TABLE system.named_collections, ALL EXCEPT DATABASES system TO ${backup_name}
" | grep -c "BACKUP_CREATED"

echo "the backup entry is FileEncryption ciphertext without the plaintext statement"
entry_file="${backup_nc_path}/${collection}.sql.enc"
head -c 3 "${entry_file}"
echo
grep -c "nc_secret_value" "${entry_file}" || true

echo "config-defined collections are not in the backup"
find "${backup_nc_path}" -name "*s3_conn*" | grep -c "s3_conn" || true

echo "a backup requires SHOW NAMED COLLECTIONS SECRETS even with the encrypted storage"
backup_user="backup_user_05401_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -m --query "
DROP USER IF EXISTS ${backup_user};
CREATE USER ${backup_user};
GRANT SHOW NAMED COLLECTIONS ON * TO ${backup_user};
GRANT displaySecretsInShowAndSelect ON *.* TO ${backup_user};
GRANT BACKUP ON *.* TO ${backup_user};
GRANT WRITE ON DISK TO ${backup_user};
"
${CLICKHOUSE_CLIENT} --user "${backup_user}" --query "
    BACKUP TABLE system.named_collections TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_nosecrets')
" 2>&1 | grep -m1 -c "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW NAMED COLLECTIONS SECRETS ON * TO ${backup_user}"
${CLICKHOUSE_CLIENT} --user "${backup_user}" --query "
    BACKUP TABLE system.named_collections TO Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}_secrets')
" | grep -c "BACKUP_CREATED"
grep -c "nc_secret_value" "${CLICKHOUSE_DISKS_FILES}/backups/${CLICKHOUSE_TEST_UNIQUE_NAME}_secrets/data/system/named_collections/${collection}.sql.enc" || true
${CLICKHOUSE_CLIENT} --query "DROP USER ${backup_user}"

echo "restore after dropping everything"
${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE nc_url_table;
DROP VIEW nc_url_view;
DROP TABLE nc_data_table;
DROP NAMED COLLECTION ${collection};
"
${CLICKHOUSE_CLIENT} --query "RESTORE ALL FROM ${backup_name}" | grep -c "RESTORED"
${CLICKHOUSE_CLIENT} --query "
    SELECT name = 'nc_${CLICKHOUSE_DATABASE}', mapKeys(collection), collection['url'], source
    FROM system.named_collections WHERE name = '${collection}'
"
${CLICKHOUSE_CLIENT} --query "
    SELECT create_query LIKE '%[HIDDEN]%' AND create_query NOT LIKE '%nc_secret_value%'
    FROM system.named_collections WHERE name = '${collection}'
"
${CLICKHOUSE_CLIENT} --query "
    SELECT create_table_query LIKE 'CREATE TABLE%URL(${collection})%'
    FROM system.tables WHERE database = currentDatabase() AND name = 'nc_url_table'
"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM nc_data_table"

echo "the restored tables hold the collection through the DROP guard"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION ${collection}" 2>&1 | grep -m1 -c "NAMED_COLLECTION_IS_USED"
${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE nc_url_table;
DROP VIEW nc_url_view;
DROP TABLE nc_data_table;
"

echo "an existing collection is kept with create_named_collection = 'if-not-exists' (the default)"
${CLICKHOUSE_CLIENT} -m --query "
DROP NAMED COLLECTION ${collection};
CREATE NAMED COLLECTION ${collection} AS url = 'http://localhost:8123/changed', marker = '1';
"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE system.named_collections FROM ${backup_name}" | grep -c "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT mapContains(collection, 'marker') FROM system.named_collections WHERE name = '${collection}'"

echo "an existing collection fails the restore with create_named_collection = 'create', without the name"
error=$(${CLICKHOUSE_CLIENT} --query "
    RESTORE TABLE system.named_collections FROM ${backup_name} SETTINGS create_named_collection = 'create'
" 2>&1)
echo "${error}" | grep -m1 -c "NAMED_COLLECTION_ALREADY_EXISTS"
echo "${error}" | grep -c "${collection}" || true
# The original name-bearing error is recorded into the error statistics on construction, before
# the sanitizing catch runs, so the restore must suppress that record.
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.errors
    WHERE name = 'NAMED_COLLECTION_ALREADY_EXISTS' AND last_error_message LIKE '%${collection}%'
"

echo "an existing collection is replaced with create_named_collection = 'replace'"
${CLICKHOUSE_CLIENT} --query "
    RESTORE TABLE system.named_collections FROM ${backup_name} SETTINGS create_named_collection = 'replace'
" | grep -c "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT mapContains(collection, 'marker') FROM system.named_collections WHERE name = '${collection}'"

# Restoring does not require `SHOW NAMED COLLECTIONS`, and `send_logs_level` streams the query's
# log records to the restoring client, so the records must name no collection.
echo "the restore streams no collection name to the client log"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION ${collection}"
CLICKHOUSE_CLIENT_WITH_LOG=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=trace/g')
logs=$(${CLICKHOUSE_CLIENT_WITH_LOG} --query "RESTORE TABLE system.named_collections FROM ${backup_name}" 2>&1 >/dev/null)
echo "${logs}" | grep -c "${collection}" || true

echo "restore_named_collections = false skips the collections"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION ${collection}"
${CLICKHOUSE_CLIENT} --query "
    RESTORE TABLE system.named_collections FROM ${backup_name} SETTINGS restore_named_collections = false
" | grep -c "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.named_collections WHERE name = '${collection}'"

echo "structure_only still restores the collections"
${CLICKHOUSE_CLIENT} --query "RESTORE ALL FROM ${backup_name} SETTINGS structure_only = true" | grep -c "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.named_collections WHERE name = '${collection}'"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM nc_data_table"

echo "restore is refused without the CREATE NAMED COLLECTION privilege, before anything is created"
user="user_05401_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -m --query "
DROP USER IF EXISTS ${user};
CREATE USER ${user};
GRANT ALL ON *.* TO ${user} WITH GRANT OPTION;
REVOKE CREATE NAMED COLLECTION ON * FROM ${user};
"
${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE nc_url_table;
DROP VIEW nc_url_view;
DROP TABLE nc_data_table;
DROP NAMED COLLECTION ${collection};
"
${CLICKHOUSE_CLIENT} --user "${user}" --query "RESTORE ALL FROM ${backup_name}" 2>&1 | grep -m1 -c "ACCESS_DENIED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.named_collections WHERE name = '${collection}'"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'nc_data_table'"

echo "the same restore succeeds with the privilege"
${CLICKHOUSE_CLIENT} --query "GRANT CREATE NAMED COLLECTION ON * TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --query "RESTORE ALL FROM ${backup_name}" | grep -c "RESTORED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM nc_data_table"

${CLICKHOUSE_CLIENT} -m --query "
DROP USER ${user};
DROP TABLE nc_url_table;
DROP VIEW nc_url_view;
DROP TABLE nc_data_table;
DROP NAMED COLLECTION ${collection};
"
