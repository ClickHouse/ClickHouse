#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: requires MinIO; no-parallel: enables a server-wide cancellation failpoint.

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

failpoint="backup_cancel_after_lock_file_creation"
destination="S3('http://localhost:11111/test/backups/${CLICKHOUSE_TEST_UNIQUE_NAME}', 'test', 'testtest')"
cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${failpoint}"
    ${CLICKHOUSE_CLIENT} --multiquery --query "DROP TABLE IF EXISTS data; DROP TABLE IF EXISTS restored"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE TABLE data (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO data VALUES (42);
SYSTEM ENABLE FAILPOINT ${failpoint};
"

# Cancel after the lock upload, while the backup constructor still owns its cleanup.
if error="$(${CLICKHOUSE_CLIENT} --send_logs_level error --query "BACKUP TABLE data TO ${destination}" 2>&1)"; then
    echo 'Expected backup cancellation'
    exit 1
fi
if [[ "$error" != *QUERY_WAS_CANCELLED* ]]; then
    echo "$error"
    exit 1
fi
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${failpoint}"

# A fresh attempt can use the same destination only if the cancelled attempt released its lock.
${CLICKHOUSE_CLIENT} --send_logs_level error --query "BACKUP TABLE data TO ${destination} FORMAT Null"
${CLICKHOUSE_CLIENT} --send_logs_level error --query "RESTORE TABLE data AS restored FROM ${destination} FORMAT Null"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM restored"
