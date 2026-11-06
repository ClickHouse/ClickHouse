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

# An exported entry is the stored FileEncryption bytes followed by a 32-byte authentication code,
# so any modification of the ciphertext must fail the authentication check before anything is
# decrypted or parsed - the unauthenticated CTR ciphertext alone could be reshaped with known
# plaintext positions without holding the key.
echo "corrupt the first ciphertext byte of the entry (after the 64-byte FileEncryption header)"
byte=$(od -An -tu1 -j64 -N1 "${entry_file}" | tr -d ' ')
printf '%b' "$(printf '\\%03o' $((byte ^ 1)))" | dd of="${entry_file}" bs=1 seek=64 count=1 conv=notrunc 2>/dev/null

echo "the restore rejects the modified entry without leaking the collection name"
${CLICKHOUSE_CLIENT} --query "DROP NAMED COLLECTION ${collection}"
error=$(${CLICKHOUSE_CLIENT} --query "RESTORE TABLE system.named_collections FROM ${backup_name}" 2>&1)
echo "${error}" | grep -m1 -c "failed authentication"
echo "${error}" | grep -c "${secret}" || true
echo "${error}" | grep -c "${collection}" || true
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.named_collections WHERE name = '${collection}'"

# Recompute the authentication code over the corrupted bytes with the test key of
# tests/config/config.d/named_collections_storage.xml; this doubles as a check of the HMAC
# construction. The authenticated entry now decrypts into a statement with a garbled first
# character - the rest, including the secret value, decrypts unchanged - and reaches the parser,
# so any leak of the parsed text into the error would be caught below.
echo "recompute the authentication code, the restore then fails in the parser without leaking"
python3 -c "
import hmac, hashlib, sys
path = sys.argv[1]
key = bytes.fromhex('5e4acb1bce2688abbc7f546d7877f04d')
data = open(path, 'rb').read()
stored = data[:-32]
auth_key = hmac.new(key, b'ClickHouse named collections backup entry v1', hashlib.sha256).digest()
mac = hmac.new(auth_key, stored, hashlib.sha256).digest()
open(path, 'wb').write(stored + mac)
" "${entry_file}"
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

# The sanitized errors above replace originals that are recorded into the error statistics when
# they are constructed, before any catch runs: the parser error embeds the decrypted statement,
# and the backup read error embeds the entry path. The restore must suppress those records.
echo "no error-statistics row holds the secret or the collection name"
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM system.errors
    WHERE last_error_message LIKE '%${secret}%' OR last_error_message LIKE '%${collection}%'
"
