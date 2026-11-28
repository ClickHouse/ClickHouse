#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# The columns of the change data feed are added by the schema transform of each data file, so a filter on them
# must not be used by the Parquet reader: a data file of `cdf-table-with-cdc-and-dvs` has an incidental physical
# `_change_type` column with only NULLs, while the transform supplies `delete` for its rows in version 10.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="${CUR_DIR}/data_delta_lake/kernel_data/cdf-table-with-cdc-and-dvs"

for push_down in 0 1; do
$CLICKHOUSE_LOCAL -q "
SELECT '--- input_format_parquet_filter_push_down = ${push_down}';
SELECT id, _change_type, _commit_version FROM deltaLakeLocal('${DATA}')
WHERE _change_type = 'delete' ORDER BY ALL
SETTINGS delta_lake_snapshot_start_version = 10, delta_lake_snapshot_end_version = 10, input_format_parquet_filter_push_down = ${push_down};

SELECT id, _change_type, _commit_version FROM deltaLakeLocal('${DATA}')
WHERE _change_type IN ('insert', 'delete', 'update_preimage', 'update_postimage') ORDER BY ALL
SETTINGS delta_lake_snapshot_start_version = 10, delta_lake_snapshot_end_version = 10, input_format_parquet_filter_push_down = ${push_down};

SELECT count() FROM deltaLakeLocal('${DATA}') WHERE _change_type = 'delete'
SETTINGS delta_lake_snapshot_start_version = 0, input_format_parquet_filter_push_down = ${push_down};
"
done
