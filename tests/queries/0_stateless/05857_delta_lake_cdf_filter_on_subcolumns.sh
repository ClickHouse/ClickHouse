#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# A filter on a subcolumn that exists only in ClickHouse, such as the null map of a `Nullable` column, must not be
# passed to delta-kernel: it takes `birthday.null` for the field `null` of a struct `birthday` and throws
# `Predicate references unknown column`. The scan of the change data feed also honors `delta_lake_enable_engine_predicate`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="${CUR_DIR}/data_delta_lake/kernel_data"

for engine_predicate in 0 1; do
$CLICKHOUSE_LOCAL -q "
SET delta_lake_enable_engine_predicate = ${engine_predicate};
SELECT '--- delta_lake_enable_engine_predicate = ${engine_predicate}';

SELECT count() FROM deltaLakeLocal('${DATA}/cdf-table')
SETTINGS delta_lake_snapshot_start_version = 0;

SELECT count() FROM deltaLakeLocal('${DATA}/cdf-table') WHERE birthday.null = 0
SETTINGS delta_lake_snapshot_start_version = 0;

SELECT count() FROM deltaLakeLocal('${DATA}/cdf-table') WHERE NOT birthday.null AND id > 0 AND _commit_version >= 0
SETTINGS delta_lake_snapshot_start_version = 0;

SELECT count() FROM deltaLakeLocal('${DATA}/cdf-table-with-cdc-and-dvs') WHERE comment.null = 0
SETTINGS delta_lake_snapshot_start_version = 0;

SELECT count() FROM deltaLakeLocal('${DATA}/cdf-table-with-cdc-and-dvs') WHERE comment.null = 1 OR id < 0
SETTINGS delta_lake_snapshot_start_version = 0;

-- The same filter on a snapshot.
SELECT count() FROM deltaLakeLocal('${DATA}/cdf-table') WHERE birthday.null = 0
SETTINGS delta_lake_throw_on_engine_predicate_error = 1;
"
done
