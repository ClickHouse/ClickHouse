#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# In the server, a relative path of a local Delta Lake table is relative to `user_files_path`, the same as for
# the `file` table function, and it must not lead outside of it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

REL="${CLICKHOUSE_TEST_UNIQUE_NAME}_tbl"
rm -rf "${USER_FILES_PATH:?}/${REL}"
trap 'rm -rf "${USER_FILES_PATH:?}/${REL}"' EXIT

SETTINGS="--allow_experimental_delta_kernel_rs=1 --allow_delta_lake_writes=1 --allow_delta_lake_create_table=1"

echo "--- create and write through a relative path"
${CLICKHOUSE_CLIENT} ${SETTINGS} -q "
CREATE TABLE t_rel (id Int32, s String) ENGINE = DeltaLakeLocal('${REL}', Parquet);
INSERT INTO t_rel SELECT number, toString(number) FROM numbers(3);
INSERT INTO t_rel SELECT number + 3, toString(number + 3) FROM numbers(2);
SELECT * FROM t_rel ORDER BY id;
DROP TABLE t_rel;
"

test -d "${USER_FILES_PATH}/${REL}/_delta_log" && echo "created in user_files_path"

echo "--- read through different spellings of the relative path"
${CLICKHOUSE_CLIENT} ${SETTINGS} -q "
SELECT count(), sum(id) FROM deltaLakeLocal('${REL}');
SELECT count(), sum(id) FROM deltaLakeLocal('./${REL}/');
SELECT count(), sum(id) FROM deltaLakeLocal('${USER_FILES_PATH}/${REL}');
"

echo "--- a relative path leading outside of user_files_path"
${CLICKHOUSE_CLIENT} ${SETTINGS} -q "SELECT count() FROM deltaLakeLocal('../${REL}')" 2>&1 | grep -o -m1 'PATH_ACCESS_DENIED'
