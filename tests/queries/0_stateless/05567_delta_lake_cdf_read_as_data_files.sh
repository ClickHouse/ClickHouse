#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so DeltaLakeLocal is absent.

# The change data feed (`delta_lake_snapshot_start_version`) is read like a snapshot: delta-kernel-rs only lists
# the data files, which ClickHouse reads with its own Parquet reader. Check the queries that read no column of
# the data files (only the columns added by the transform of each file), the virtual columns, and that the result
# does not depend on the number of threads. `cdf-table-with-cdc-and-dvs` has CDC files (with a physical
# `_change_type` column), and deletion vectors which exclude rows or, in pairs, select the changed rows.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="${CUR_DIR}/data_delta_lake/kernel_data"

$CLICKHOUSE_LOCAL -q "
SELECT '--- count';
SELECT count() FROM deltaLakeLocal('${DATA}/cdf-table-with-cdc-and-dvs') SETTINGS delta_lake_snapshot_start_version = 0;
SELECT count() FROM deltaLakeLocal('${DATA}/cdf-table') SETTINGS delta_lake_snapshot_start_version = 0;

SELECT '--- only the columns of the change data feed';
SELECT _change_type, _commit_version, count() FROM deltaLakeLocal('${DATA}/cdf-table-with-cdc-and-dvs')
GROUP BY ALL ORDER BY ALL SETTINGS delta_lake_snapshot_start_version = 0;

SELECT '--- only a partition column';
SELECT birthday, count() FROM deltaLakeLocal('${DATA}/cdf-table')
GROUP BY ALL ORDER BY ALL SETTINGS delta_lake_snapshot_start_version = 0;

SELECT '--- virtual columns';
SELECT count(DISTINCT _path) > 1, countIf(_file NOT LIKE '%.parquet') FROM deltaLakeLocal('${DATA}/cdf-table-with-cdc-and-dvs')
SETTINGS delta_lake_snapshot_start_version = 0;

SELECT '--- a subcolumn';
SELECT struct_col.field, _change_type FROM deltaLakeLocal('${DATA}/cdf-table-backtick-column-names')
ORDER BY ALL SETTINGS delta_lake_snapshot_start_version = 0;

SELECT '--- the number of threads';
SELECT
    (SELECT groupArraySorted(1000)((id, comment, _change_type, _commit_version)) FROM deltaLakeLocal('${DATA}/cdf-table-with-cdc-and-dvs')
     SETTINGS delta_lake_snapshot_start_version = 0, max_threads = 1)
    = (SELECT groupArraySorted(1000)((id, comment, _change_type, _commit_version)) FROM deltaLakeLocal('${DATA}/cdf-table-with-cdc-and-dvs')
     SETTINGS delta_lake_snapshot_start_version = 0, max_threads = 8);
"

# The TopN query condition cache of a snapshot read must not be used by a read of the change data feed, which reads other
# data files at the same pinned snapshot: the snapshot read skips the data file of version 1 (ids 2, 3, 4), which the
# change data feed of version 1 reads.
$CLICKHOUSE_LOCAL -q "
DROP DATABASE IF EXISTS ${CLICKHOUSE_DATABASE}_cache;
CREATE DATABASE ${CLICKHOUSE_DATABASE}_cache ENGINE = Atomic;
CREATE TABLE ${CLICKHOUSE_DATABASE}_cache.t ENGINE = DeltaLakeLocal('${DATA}/cdf-table');
SET use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 1;
SELECT '--- TopN query condition cache';
SELECT id FROM ${CLICKHOUSE_DATABASE}_cache.t ORDER BY id DESC LIMIT 1;
SELECT id FROM ${CLICKHOUSE_DATABASE}_cache.t ORDER BY id DESC LIMIT 1;
SELECT id FROM ${CLICKHOUSE_DATABASE}_cache.t ORDER BY id DESC LIMIT 1 SETTINGS delta_lake_snapshot_start_version = 1, delta_lake_snapshot_end_version = 1;
DROP DATABASE ${CLICKHOUSE_DATABASE}_cache;
"
