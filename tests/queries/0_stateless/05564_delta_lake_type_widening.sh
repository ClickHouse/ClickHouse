#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# Type widening (the `typeWidening` table feature) in tables written by Spark: data files written before the
# widening keep the narrow physical types. The types of every version, filters on widened columns (the statistics
# of old files are of the narrow type), widened nested fields, array elements, map keys and values, and a widened
# partition column. See `05560_delta_lake_corpus`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

T="$CUR_DIR/data_lake_corpus/delta/type_widening"
N="$CUR_DIR/data_lake_corpus/delta/type_widening_nested"

$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT '-- before widening';
DESCRIBE deltaLakeLocal('$T') SETTINGS delta_lake_snapshot_version = 1;
SELECT * FROM deltaLakeLocal('$T') ORDER BY id SETTINGS delta_lake_snapshot_version = 1;

SELECT '-- after one column is widened';
DESCRIBE deltaLakeLocal('$T') SETTINGS delta_lake_snapshot_version = 2;

SELECT '-- after widening';
DESCRIBE deltaLakeLocal('$T');
SELECT * FROM deltaLakeLocal('$T') ORDER BY id;
SELECT toTypeName(sum(i)), sum(i), sum(sh), sum(by), sum(dec) FROM deltaLakeLocal('$T');

SELECT '-- filters on widened columns';
SELECT id FROM deltaLakeLocal('$T') WHERE i > 5000000000;
SELECT id FROM deltaLakeLocal('$T') WHERE i = -2147483648;
SELECT id FROM deltaLakeLocal('$T') WHERE sh = 100000;
SELECT id FROM deltaLakeLocal('$T') WHERE sh < -30000;
SELECT id FROM deltaLakeLocal('$T') WHERE by = 1000;
SELECT id FROM deltaLakeLocal('$T') WHERE f > 1e100;
SELECT id FROM deltaLakeLocal('$T') WHERE dec > 10000;
SELECT id FROM deltaLakeLocal('$T') WHERE dt >= '2020-01-01' ORDER BY id;
SELECT id FROM deltaLakeLocal('$T') WHERE dt = '2020-01-01 00:00:00';
SELECT id FROM deltaLakeLocal('$T') WHERE i IS NULL;

SELECT '-- nested fields, array elements, map keys and values, partition column';
DESCRIBE deltaLakeLocal('$N') SETTINGS delta_lake_snapshot_version = 1;
DESCRIBE deltaLakeLocal('$N');
SELECT * FROM deltaLakeLocal('$N') ORDER BY id;
-- Subcolumns (\`st.a\`) of a partitioned table are not read here: they fail with UNKNOWN_IDENTIFIER (see \`05560_delta_lake_corpus\`).
SELECT id, toTypeName(st), st, toTypeName(arr), arr, toTypeName(mp), mp FROM deltaLakeLocal('$N') ORDER BY id;
SELECT id FROM deltaLakeLocal('$N') WHERE p = 100000;
SELECT id FROM deltaLakeLocal('$N') WHERE p = 1;
SELECT id FROM deltaLakeLocal('$N') WHERE tupleElement(st, 'a') > 70000 SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id FROM deltaLakeLocal('$N') WHERE has(arr, 10000000000);
SELECT p, count() FROM deltaLakeLocal('$N') GROUP BY p ORDER BY p;
"
