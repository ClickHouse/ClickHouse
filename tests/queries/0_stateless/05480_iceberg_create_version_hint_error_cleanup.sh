#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# Tag no-fasttest: requires `IcebergLocal` (USE_AVRO).
# Tag no-parallel: toggles a process-global failpoint.

# A `CREATE` whose version hint write fails after the hint was stored must remove the hint
# together with the metadata file, so that retrying the `CREATE` at the same location succeeds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE="t_${CLICKHOUSE_DATABASE}_${RANDOM}"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"

trap "rm -rf \"${TABLE_PATH}\" 2>/dev/null; ${CLICKHOUSE_CLIENT} --query \"SYSTEM DISABLE FAILPOINT iceberg_create_fail_after_version_hint_write\" 2>/dev/null" EXIT

CREATE_QUERY="CREATE TABLE ${TABLE} (c0 Int32) ENGINE = IcebergLocal('${TABLE_PATH}') SETTINGS iceberg_use_version_hint = 1"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT iceberg_create_fail_after_version_hint_write"
${CLICKHOUSE_CLIENT} --query "${CREATE_QUERY}" 2>&1 | grep -o -m1 "Failpoint: the check after the upload of the version hint failed" | head -n1

echo "Files left: $(find "${TABLE_PATH}" -type f 2>/dev/null | wc -l)"

${CLICKHOUSE_CLIENT} --query "${CREATE_QUERY}"
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "INSERT INTO ${TABLE} VALUES (1)"
${CLICKHOUSE_CLIENT} --query "SELECT c0 FROM ${TABLE}"
${CLICKHOUSE_CLIENT} --query "DROP TABLE ${TABLE}"
