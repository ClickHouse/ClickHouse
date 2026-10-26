#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# no-fasttest: requires the MinIO S3 endpoint, and the encrypted storage for named collections
#              requires SSL, so tests/config/config.d/named_collections_storage.xml is not
#              installed there.
# no-parallel: `BACKUP TABLE system.named_collections` captures every SQL-created collection on the
#              server, so a collection dropped by a concurrent test could fail the backup.
# no-replicated-database: named collections are server-global, not database-scoped.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

collection="nc_${CLICKHOUSE_DATABASE}"
secret="nc_secret_value_${CLICKHOUSE_DATABASE}"
backup_path="backups/${CLICKHOUSE_TEST_UNIQUE_NAME}"
backup_name="S3('http://localhost:11111/test/${backup_path}', 'test', 'testtest')"
entry_key="${backup_path}/data/system/named_collections/${collection}.sql.enc"
restore_user="restore_user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

# A clickhouse-disks configuration rooted at the bucket, to delete a single backup object.
disks_config="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_disks.xml"
cat > "${disks_config}" <<EOF
<clickhouse>
    <storage_configuration>
        <disks>
            <s3_backup_bucket>
                <type>s3_plain</type>
                <endpoint>http://localhost:11111/test/</endpoint>
                <access_key_id>clickhouse</access_key_id>
                <secret_access_key>clickhouse</secret_access_key>
            </s3_backup_bucket>
        </disks>
    </storage_configuration>
</clickhouse>
EOF

${CLICKHOUSE_CLIENT} -m --query "
DROP NAMED COLLECTION IF EXISTS ${collection};
CREATE NAMED COLLECTION ${collection} AS key1 = '${secret}';
"

echo "backup to S3"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.named_collections TO ${backup_name}" | grep -c "BACKUP_CREATED"

# The entry stays listed in the backup metadata, so the restore still reads it from S3 and the read
# fails. `ReadBufferFromS3` logs the object key - which is the collection's name - before the
# restore replaces the error with a sanitized one, so the restore must also keep the log records of
# the read out of the client's log stream.
echo "delete the entry object from S3"
clickhouse-disks -C "${disks_config}" --disk s3_backup_bucket --query "remove ${entry_key}"
clickhouse-disks -C "${disks_config}" --disk s3_backup_bucket --query "list ${backup_path}/data/system/named_collections/" | grep -c "${collection}" || true

# Restoring requires neither `SHOW NAMED COLLECTIONS` nor `SHOW NAMED COLLECTIONS SECRETS`: an
# operator must be able to restore a backup without being able to read it.
${CLICKHOUSE_CLIENT} -m --query "
DROP USER IF EXISTS ${restore_user};
CREATE USER ${restore_user};
GRANT CREATE NAMED COLLECTION ON * TO ${restore_user};
GRANT READ ON S3 TO ${restore_user};
"

echo "the restore fails without naming the collection in the error or in the streamed logs"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION ${collection}"
CLICKHOUSE_CLIENT_WITH_LOG=$(echo ${CLICKHOUSE_CLIENT} | sed 's/'"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"'/--send_logs_level=trace/g')
output=$(${CLICKHOUSE_CLIENT_WITH_LOG} --user "${restore_user}" --query "
    RESTORE TABLE system.named_collections FROM ${backup_name}
" 2>&1)
echo "${output}" | grep -m1 -c "Cannot read a named collection entry"
echo "${output}" | grep -c "${collection}" || true
echo "${output}" | grep -c "${secret}" || true
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.named_collections WHERE name = '${collection}'"

${CLICKHOUSE_CLIENT} --query "DROP USER ${restore_user}"
