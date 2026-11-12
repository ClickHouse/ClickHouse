#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# no-fasttest: `deltaLakeLocal` needs the delta-kernel-rs library, which fast test does not build.
# no-msan: delta-kernel-rs is Rust code that is not instrumented by MSan.

# Delta table features on tables adopted from the delta-rs test suite (see data_delta_lake/delta_rs/README.md):
# deletion vectors with checkpoints, in-commit timestamps, liquid clustering with row tracking,
# INT96 timestamps beyond the nanosecond range, an unknown reader feature and the Variant type.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_delta_lake/delta_rs"

$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT '-- table_with_deletion_logs: 100 rows, a deletion vector removes ids 12 and 89, checkpoints at 10 and 20';
DESCRIBE deltaLakeLocal('$DATA/table_with_deletion_logs');
SELECT count(), min(id), max(id), sum(id) FROM deltaLakeLocal('$DATA/table_with_deletion_logs');
SELECT count() FROM deltaLakeLocal('$DATA/table_with_deletion_logs') WHERE id IN (12, 89);
SELECT id, name, nbr FROM deltaLakeLocal('$DATA/table_with_deletion_logs') WHERE id BETWEEN 10 AND 14 OR id BETWEEN 87 AND 91 ORDER BY id;
SELECT count() FROM deltaLakeLocal('$DATA/table_with_deletion_logs') SETTINGS delta_lake_snapshot_version = 0;

SELECT '-- table_with_ict: in-commit timestamps';
SELECT * FROM deltaLakeLocal('$DATA/table_with_ict') ORDER BY ALL;
SELECT groupArray(id) FROM (SELECT id FROM deltaLakeLocal('$DATA/table_with_ict') ORDER BY id) SETTINGS delta_lake_snapshot_version = 2;

SELECT '-- table_with_liquid_clustering: liquid clustering, row tracking, deletion vectors and domain metadata enabled';
DESCRIBE deltaLakeLocal('$DATA/table_with_liquid_clustering');
SELECT groupArray(id) FROM (SELECT id FROM deltaLakeLocal('$DATA/table_with_liquid_clustering') ORDER BY id);
SELECT count() FROM deltaLakeLocal('$DATA/table_with_liquid_clustering') WHERE id >= 5;

SELECT '-- table_with_edge_timestamps: columns inside the DateTime64 range';
DESCRIBE deltaLakeLocal('$DATA/table_with_edge_timestamps');
SELECT NORMAL_DATE, SOME_VALUE FROM deltaLakeLocal('$DATA/table_with_edge_timestamps') ORDER BY SOME_VALUE;
"

# BIG_DATE holds 9999-12-31 and 9999-12-30 as INT96, outside the Int64 nanosecond range.
echo "-- table_with_edge_timestamps: INT96 timestamps beyond the nanosecond range read into DateTime64(6)"
$CLICKHOUSE_LOCAL --session_timezone UTC -q "SELECT BIG_DATE FROM deltaLakeLocal('$DATA/table_with_edge_timestamps') ORDER BY SOME_VALUE"

# The protocol lists a reader feature `blahabl` that no reader knows, so the table must be refused.
echo "-- simple_table_features: unknown reader feature"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM deltaLakeLocal('$DATA/simple_table_features')" 2>&1 | grep -o -m1 'DELTA_KERNEL_ERROR'

# The Variant type is not supported by the Delta Lake reader yet.
echo "-- spark-variant-checkpoint: Variant column"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM deltaLakeLocal('$DATA/spark-variant-checkpoint')" 2>&1 | grep -o -m1 'NOT_IMPLEMENTED'
