#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# With `delta_lake_reload_schema_for_consistency`, the schema is rebuilt for the pinned version of the table.
# For the change data feed, it must be the schema of the change data feed, with `_change_type` and other columns.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="${CUR_DIR}/data_delta_lake/kernel_data/cdf-table-with-cdc-and-dvs"

for reload in 0 1; do
    $CLICKHOUSE_LOCAL -q "
    SELECT _change_type, _commit_version, count() FROM deltaLakeLocal('${DATA}')
    GROUP BY ALL ORDER BY ALL
    SETTINGS delta_lake_snapshot_start_version = 0, delta_lake_reload_schema_for_consistency = $reload" > "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_$reload.out" 2>&1
done
cat "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_1.out"
cmp -s "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_0.out" "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}_1.out" && echo 'same result'

$CLICKHOUSE_LOCAL -q "
SELECT count() FROM deltaLakeLocal('${DATA}') WHERE _change_type = 'insert'
SETTINGS delta_lake_snapshot_start_version = 0, delta_lake_snapshot_end_version = 0, delta_lake_reload_schema_for_consistency = 1"

rm -f "${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"_{0,1}.out
