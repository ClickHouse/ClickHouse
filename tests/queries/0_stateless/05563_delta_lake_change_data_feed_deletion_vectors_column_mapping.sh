#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# The change data feed (`delta_lake_snapshot_start_version` and `delta_lake_snapshot_end_version`) of tables
# written by Spark with deletion vectors (UPDATE and DELETE write deletion vectors and `cdc` files in the same
# commit) and with column mapping in `name` mode on a partitioned table. See `05560_delta_lake_corpus`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DV="$CUR_DIR/data_lake_corpus/delta/change_data_feed_deletion_vectors"
CM="$CUR_DIR/data_lake_corpus/delta/change_data_feed_column_mapping"
OLD="$CUR_DIR/data_lake_corpus/delta/delta332_change_data_feed_deletion_vectors"

$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT '-- deletion vectors: all changes';
SELECT _commit_version, _change_type, id, s, p FROM deltaLakeLocal('$DV') ORDER BY _commit_version, id, _change_type SETTINGS delta_lake_snapshot_start_version = 0;

SELECT '-- one version';
SELECT _commit_version, _change_type, id, s FROM deltaLakeLocal('$DV') ORDER BY id, _change_type SETTINGS delta_lake_snapshot_start_version = 3, delta_lake_snapshot_end_version = 3;

SELECT '-- range, filters, aggregation';
SELECT _change_type, count(), sum(id) FROM deltaLakeLocal('$DV') GROUP BY _change_type ORDER BY _change_type SETTINGS delta_lake_snapshot_start_version = 2, delta_lake_snapshot_end_version = 4;
SELECT count() FROM deltaLakeLocal('$DV') WHERE _change_type = 'delete' SETTINGS delta_lake_snapshot_start_version = 0;
SELECT id, s FROM deltaLakeLocal('$DV') WHERE _change_type = 'update_postimage' AND id = 2 SETTINGS delta_lake_snapshot_start_version = 0;
SELECT count() FROM deltaLakeLocal('$DV') SETTINGS delta_lake_snapshot_start_version = 4;
-- Without in-commit timestamps, the commit timestamp is the modification time of the commit file, which is not fixed in a checkout.
SELECT uniqExact(_commit_version), min(_commit_timestamp) > '2000-01-01' FROM deltaLakeLocal('$DV') SETTINGS delta_lake_snapshot_start_version = 1;

SELECT '-- the snapshot is not affected by the change data feed';
SELECT count(), sum(id) FROM deltaLakeLocal('$DV');

SELECT '-- older writer';
SELECT
    (SELECT groupArray((_commit_version, _change_type, id, s, p)) FROM (SELECT * FROM deltaLakeLocal('$DV') ORDER BY ALL) SETTINGS delta_lake_snapshot_start_version = 0)
  = (SELECT groupArray((_commit_version, _change_type, id, s, p)) FROM (SELECT * FROM deltaLakeLocal('$OLD') ORDER BY ALL) SETTINGS delta_lake_snapshot_start_version = 0);

SELECT '-- column mapping, partitioned';
DESCRIBE deltaLakeLocal('$CM') SETTINGS delta_lake_snapshot_start_version = 0;
SELECT _commit_version, _change_type, id, s, p FROM deltaLakeLocal('$CM') ORDER BY _commit_version, id, _change_type SETTINGS delta_lake_snapshot_start_version = 0;
SELECT _commit_version, _change_type, id FROM deltaLakeLocal('$CM') WHERE p = 0 ORDER BY ALL SETTINGS delta_lake_snapshot_start_version = 1;
"

echo '-- invalid ranges'
$CLICKHOUSE_LOCAL -q "SELECT count() FROM deltaLakeLocal('$DV') SETTINGS delta_lake_snapshot_start_version = 3, delta_lake_snapshot_end_version = 2" 2>&1 | grep -o -m1 -E '\([A-Z_]+\)'
$CLICKHOUSE_LOCAL -q "SELECT count() FROM deltaLakeLocal('$DV') SETTINGS delta_lake_snapshot_start_version = 100" 2>&1 | grep -o -m1 -E '\([A-Z_]+\)'
