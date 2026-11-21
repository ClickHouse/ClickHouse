#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# A relative path of a local Delta Lake table is relative to the current directory in `clickhouse-local`,
# for the delta-kernel-rs reader and writer, the same as for the `file` table function.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

BASE_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${BASE_DIR}"
mkdir -p "${BASE_DIR}/work"
trap 'rm -rf "${BASE_DIR}"' EXIT

cd "${BASE_DIR}/work" || exit 1

SETTINGS="--allow_delta_lake_writes=1 --allow_delta_lake_create_table=1"

echo "--- create and write through a relative path"
${CLICKHOUSE_LOCAL} ${SETTINGS} -q "
CREATE TABLE t (id Int32, s String) ENGINE = DeltaLakeLocal('tbl', Parquet);
INSERT INTO t SELECT number, toString(number) FROM numbers(3);
INSERT INTO t SELECT number + 3, toString(number + 3) FROM numbers(2);
SELECT * FROM t ORDER BY id;
"

# The table is created in the current directory and not anywhere else.
test -d "${BASE_DIR}/work/tbl/_delta_log" && echo "created in the current directory"

echo "--- read through different spellings of the relative path"
${CLICKHOUSE_LOCAL} ${SETTINGS} -q "
SELECT count(), sum(id) FROM deltaLakeLocal('tbl');
SELECT count(), sum(id) FROM deltaLakeLocal('tbl/');
SELECT count(), sum(id) FROM deltaLakeLocal('./tbl');
SELECT count(), sum(id) FROM deltaLakeLocal('../work/tbl/');
"

echo "--- the absolute path reads the same table"
${CLICKHOUSE_LOCAL} ${SETTINGS} -q "SELECT count(), sum(id) FROM deltaLakeLocal('${BASE_DIR}/work/tbl')"

echo "--- a table written by Spark, read through a relative path"
cd "${CUR_DIR}/data_delta_lake" || exit 1
${CLICKHOUSE_LOCAL} -q "
SELECT count(), sum(c1) FROM deltaLakeLocal('lakehouses/spark_catalog/test/t0');
"
