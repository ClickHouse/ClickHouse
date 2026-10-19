#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database, no-shared-merge-tree
# Tag no-fasttest: requires MinIO and failpoints.
# Tag no-parallel: the global failpoint must not affect another projection writer.
# Tags no-replicated-database, no-shared-merge-tree: requires synchronous local `OPTIMIZE` on an S3 disk.

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

endpoint="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
table="temporary_projection_s3_rollback"
failpoint="temporary_projection_fail_after_finalize"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${failpoint}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${table} SYNC"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE TABLE ${table} (key UInt64, value String CODEC(NONE), PROJECTION p (SELECT * ORDER BY value))
ENGINE = MergeTree ORDER BY key
SETTINGS disk = disk(type = s3, endpoint = '${endpoint}', access_key_id = 'clickhouse', secret_access_key = 'clickhouse'),
    min_bytes_for_full_part_storage = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    materialize_projections_on_insert = 0, materialize_projections_on_merge = 1,
    merge_selector_algorithm = 'Manual';
INSERT INTO ${table} SELECT number, repeat(toString(number), 128) FROM numbers(1024);
"

list_objects="SELECT _path FROM s3('${endpoint}**', 'clickhouse', 'clickhouse', 'One') ORDER BY _path"
read_data="SELECT count(), sum(key), sum(sipHash64(value)) FROM ${table} SETTINGS optimize_use_projections = 0"
original_objects=$(${CLICKHOUSE_CLIENT} --query "${list_objects}")
original_data=$(${CLICKHOUSE_CLIENT} --query "${read_data}")
test -n "${original_objects}"

# Finish real non-inline projection objects, then fail before returning the temporary part to its caller.
${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${failpoint}"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE ${table} FINAL SETTINGS optimize_skip_merged_partitions = 0; -- { serverError ABORTED }"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${failpoint}"

test "$(${CLICKHOUSE_CLIENT} --query "${list_objects}")" = "${original_objects}"
test "$(${CLICKHOUSE_CLIENT} --query "${read_data}")" = "${original_data}"
echo 'Original objects and data preserved; failed projection objects removed.'
