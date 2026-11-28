#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# A CDC file of the change data feed has a physical `_change_type` column. A subcolumn of it requested without the
# column itself must still read the column from the file, because the schema transform extracts the subcolumn from it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="${CUR_DIR}/data_delta_lake/kernel_data/cdf-table"

$CLICKHOUSE_LOCAL -q "
SELECT \`_change_type.size\`, count() FROM deltaLakeLocal('${DATA}') GROUP BY ALL ORDER BY ALL
SETTINGS delta_lake_snapshot_start_version = 0;

SELECT '---';
SELECT _change_type, \`_change_type.size\`, count() FROM deltaLakeLocal('${DATA}') GROUP BY ALL ORDER BY ALL
SETTINGS delta_lake_snapshot_start_version = 0;

SELECT '---';
SELECT count() FROM deltaLakeLocal('${DATA}') WHERE \`_change_type.size\` = 6
SETTINGS delta_lake_snapshot_start_version = 0;
"
