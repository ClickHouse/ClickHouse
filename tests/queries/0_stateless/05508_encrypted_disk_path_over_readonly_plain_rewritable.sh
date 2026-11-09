#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: encrypted disks need SSL

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

work_dir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${work_dir}"
mkdir -p "${work_dir}"

# The encrypted disk creates its `path` on the disk it wraps. A read-only disk refuses that, and the CREATE must fail
# with the refusal instead of waiting for the read-only disk to accept the write; `timeout` bounds a wait that never ends.
timeout --kill-after=5 60 ${CLICKHOUSE_LOCAL} --path "${work_dir}/db" --query "
CREATE TABLE t (k Int32) ENGINE = MergeTree ORDER BY k
SETTINGS disk = disk(
    name = 'enc', type = encrypted, key = '1234567812345678', path = 'enc/',
    disk = disk(
        name = 'ro', readonly = 1,
        type = object_storage, object_storage_type = local, metadata_type = plain_rewritable,
        path = '${work_dir}/disk/'))
" -- --custom_local_disks_base_directory="${work_dir}/" > /dev/null 2> "${work_dir}/stderr"
echo "exit code: $?"
grep -o -m1 'is readonly. (READONLY)' "${work_dir}/stderr"

rm -rf "${work_dir}"
