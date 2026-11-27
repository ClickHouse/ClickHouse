#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# A filter on `_row_id` or `_last_updated_sequence_number` must not skip a data file by the values its rows inherit
# (`first_row_id` + position, the data sequence number) when the file stores some of the values itself,
# https://github.com/ClickHouse/ClickHouse/issues/130022.
# `data_iceberg/tileverse/iceberg-row-lineage/materialized` comes from the parquetry test kit (Apache-2.0),
# tileverse-io/parquetry@fba9d4db27f93fd7e21f2dd121bd81f40646347e, `internal/parquetry-testkit/src/main/resources`.
# `data_iceberg/duckdb/data/persistent/row_lineage_stored_columns` comes from the DuckDB Iceberg extension tests (MIT),
# duckdb/duckdb-iceberg@b57f45c2c78965c71f699aa23ae73c292923668e.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A randomized `use_iceberg_partition_pruning = 0` would read every file and hide the bug.
SETTINGS='SETTINGS use_iceberg_partition_pruning = 1'

cd "$CUR_DIR/data_iceberg/tileverse" || exit 1
MATERIALIZED="icebergLocal('iceberg-row-lineage/materialized')"

# File 0 (id 0..4) stores no `_row_id`. File 1 (id 5..9, `first_row_id` 5) stores `_row_id` [NULL, 1001, NULL, 1003, NULL],
# and its manifest declares the bounds [1001, 1003] with 3 nulls.
$CLICKHOUSE_LOCAL -q "
SELECT id FROM $MATERIALIZED WHERE _row_id > 1000 ORDER BY id $SETTINGS;
SELECT id FROM $MATERIALIZED WHERE _row_id = 5 ORDER BY id $SETTINGS;
SELECT id FROM $MATERIALIZED WHERE _row_id IN (5, 1003) ORDER BY id $SETTINGS;
SELECT id FROM $MATERIALIZED WHERE _row_id < 10 ORDER BY id $SETTINGS;
"

# The declared bounds of file 1 widened by its inherited row ids [5, 9] still skip it. File 0 declares no bounds and is read.
$CLICKHOUSE_LOCAL -q "
SELECT id FROM $MATERIALIZED WHERE _row_id > 2000 $SETTINGS;
SELECT value FROM system.events WHERE event = 'IcebergMinMaxIndexPrunedFiles';
"

cd "$CUR_DIR/data_iceberg/duckdb" || exit 1
STORED="icebergLocal('data/persistent/row_lineage_stored_columns/warehouse/default/row_lineage_stored_columns')"

# The first file (id 1..4) stores `_row_id` [NULL, 100, NULL, 103] and `_last_updated_sequence_number` [NULL, NULL, 7, 9],
# and its manifest declares no statistics.
$CLICKHOUSE_LOCAL -q "
SELECT id FROM $STORED WHERE _row_id > 50 ORDER BY id $SETTINGS;
SELECT id FROM $STORED WHERE _last_updated_sequence_number > 5 ORDER BY id $SETTINGS;
"
