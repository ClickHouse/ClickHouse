#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires S3

# A disk transaction that fails to commit on a `cache` disk must remove the blobs that it wrote from the object storage.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

work_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${work_dir}"
mkdir -p "${work_dir}"

url="http://localhost:11111/test/${CLICKHOUSE_TEST_UNIQUE_NAME}/"
objects="(SELECT count() FROM s3('${url}**', 'clickhouse', 'clickhouse', 'One'))"

# A private instance, so the failpoint cannot fire in other tests.
${CLICKHOUSE_LOCAL} --path "${work_dir}/db" --query "
CREATE TABLE t (x UInt64, s String) ENGINE = MergeTree ORDER BY x
SETTINGS disk = disk(
    type = cache,
    max_size = '16Mi',
    max_file_segment_size = '4Mi',
    path = 'cache/',
    disk = disk(type = s3, endpoint = '${url}', access_key_id = clickhouse, secret_access_key = clickhouse));
INSERT INTO t SELECT number, toString(number) FROM numbers(1000);
CREATE TEMPORARY TABLE objects_before ENGINE = Memory AS SELECT ${objects} AS n;

SYSTEM ENABLE FAILPOINT disk_object_storage_fail_commit_metadata_transaction;
INSERT INTO t SELECT number, toString(number) FROM numbers(1000, 1000); -- { serverError FAULT_INJECTED }

SELECT 'leaked objects', ${objects} - (SELECT n FROM objects_before);
SELECT 'rows', count(), sum(x) FROM t SETTINGS enable_filesystem_cache = 0;
DROP TABLE t SYNC;
"

rm -rf "${work_dir}"
