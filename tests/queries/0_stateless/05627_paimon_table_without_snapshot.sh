#!/usr/bin/env bash
# Tags: no-fasttest

CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL=error

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A Paimon table that was created but has no commit yet: a schema file and no snapshot.
TABLE_DIR="${CLICKHOUSE_USER_FILES_UNIQUE}/paimon_without_snapshot"
rm -rf "${TABLE_DIR}"
mkdir -p "${TABLE_DIR}/schema"
cp "${CUR_DIR}/data_minio/paimon_primary_key/schema/schema-0" "${TABLE_DIR}/schema/"

${CLICKHOUSE_CLIENT} -q "DESCRIBE paimonLocal('${TABLE_DIR}')"
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM paimonLocal('${TABLE_DIR}')"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM paimonLocal('${TABLE_DIR}')"

rm -rf "${TABLE_DIR}"
