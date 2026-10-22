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
secret="nc_secret_value_${CLICKHOUSE_DATABASE}"
backup_name="Disk('backups', '${CLICKHOUSE_TEST_UNIQUE_NAME}')"
entry_file="${CLICKHOUSE_DISKS_FILES}/backups/${CLICKHOUSE_TEST_UNIQUE_NAME}/data/system/named_collections/${collection}.sql.enc"

${CLICKHOUSE_CLIENT} -m --query "
DROP NAMED COLLECTION IF EXISTS ${collection};
CREATE NAMED COLLECTION ${collection} AS key1 = '${secret}';
"

echo "backup"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE system.named_collections TO ${backup_name}" | grep -c "BACKUP_CREATED"

# The storage's CTR encryption is not authenticated, so this leaves the entry decryptable (the key
# fingerprint of the header is intact) but garbles the first character of the statement: the entry
# reaches the parser, and the parser fails on it. The rest of the statement, including the secret
# value, decrypts unchanged, so any leak of the parsed text into the error would be caught below.
echo "corrupt the first ciphertext byte of the entry (after the 64-byte FileEncryption header)"
byte=$(od -An -tu1 -j64 -N1 "${entry_file}" | tr -d ' ')
printf '%b' "$(printf '\\%03o' $((byte ^ 1)))" | dd of="${entry_file}" bs=1 seek=64 count=1 conv=notrunc 2>/dev/null

echo "the restore fails without leaking the decrypted statement or the collection name"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION ${collection}"
error=$(${CLICKHOUSE_CLIENT} --query "RESTORE TABLE system.named_collections FROM ${backup_name}" 2>&1)
echo "${error}" | grep -m1 -c "Cannot parse a named collection entry"
echo "${error}" | grep -c "${secret}" || true
echo "${error}" | grep -c "${collection}" || true
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.named_collections WHERE name = '${collection}'"

# The entry stays listed in the backup metadata, so the restore still reads it and fails. The
# read errors of the backup implementation name the entry file, and the entry file name is the
# collection's name, so the restore must replace them with an error of its own.
echo "the restore fails without naming the entry when the entry data is missing from the backup"
rm "${entry_file}"
error=$(${CLICKHOUSE_CLIENT} --query "RESTORE TABLE system.named_collections FROM ${backup_name}" 2>&1)
echo "${error}" | grep -m1 -c "Cannot read a named collection entry"
echo "${error}" | grep -c "${collection}" || true
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.named_collections WHERE name = '${collection}'"
