#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-shared-merge-tree, no-replicated-database
# no-fasttest: requires MinIO; no-parallel: cancellation failpoint is server-wide.
# Replicated variants do not support this independently owned `plain_rewritable` disk.

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

endpoint="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT temporary_part_cancel_after_remove_rename"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS dry_run_cleanup SYNC"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE TABLE dry_run_cleanup (key UInt64, value String) ENGINE = MergeTree ORDER BY key
SETTINGS table_disk = 1, disk = disk(
    type = s3_plain_rewritable, endpoint = '${endpoint}',
    access_key_id = 'clickhouse', secret_access_key = 'clickhouse'),
    min_bytes_for_wide_part = 0;
SYSTEM STOP MERGES dry_run_cleanup;
INSERT INTO dry_run_cleanup VALUES (1, 'one');
INSERT INTO dry_run_cleanup VALUES (2, 'two');
"

objects_query="SELECT _path FROM s3('${endpoint}**', 'clickhouse', 'clickhouse', 'One') ORDER BY _path"
before="$(${CLICKHOUSE_CLIENT} --query "${objects_query}")"
test -n "$before"
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT temporary_part_cancel_after_remove_rename"

# Cancel after the temporary directory has moved: destructor cleanup cannot find its old path.
# The finished query may report cancellation; either way, its output must already be reclaimed.
if ! error="$(${CLICKHOUSE_CLIENT} --query "
    OPTIMIZE TABLE dry_run_cleanup DRY RUN PARTS 'all_1_1_0', 'all_2_2_0'
    SETTINGS optimize_dry_run_check_part = 0" 2>&1)"; then
    if [[ "$error" != *QUERY_WAS_CANCELLED* ]]; then
        echo "$error"
        exit 1
    fi
fi
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT temporary_part_cancel_after_remove_rename"
after="$(${CLICKHOUSE_CLIENT} --query "${objects_query}")"
test "$before" = "$after"
echo 'temporary output reclaimed'
${CLICKHOUSE_CLIENT} --query "SELECT * FROM dry_run_cleanup ORDER BY key"
