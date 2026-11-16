#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Row lineage of a format version 3 Iceberg table with ORC data files, written by Spark: three appends of one row
# each, then a compaction that stores `_row_id` and `_last_updated_sequence_number` in the new data file.
# https://github.com/ClickHouse/ClickHouse/issues/130032
# The table is from the Apache Impala test data (Apache License 2.0), apache/impala@e35eb8f3582449d736e74ce32f1138f5f5dc085b
# `testdata/data/iceberg_test/iceberg_v3/iceberg_v3_row_lineage_orc`, expected results of `iceberg-v3-row-lineage.test`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/impala" || exit 1

$CLICKHOUSE_LOCAL -q "
SELECT i, _row_id, _last_updated_sequence_number FROM icebergLocal('iceberg_v3/iceberg_v3_row_lineage_orc') ORDER BY i;
SELECT i, _row_id, _last_updated_sequence_number FROM icebergLocal('iceberg_v3/iceberg_v3_row_lineage_orc') ORDER BY i SETTINGS iceberg_snapshot_id = 7033898671372067760;
SELECT i, _row_id FROM icebergLocal('iceberg_v3/iceberg_v3_row_lineage_orc') ORDER BY i;
SELECT i, _last_updated_sequence_number FROM icebergLocal('iceberg_v3/iceberg_v3_row_lineage_orc') ORDER BY i;
-- The filters must give the same rows when the data files are pruned by the row lineage bounds of the manifest.
SELECT i FROM icebergLocal('iceberg_v3/iceberg_v3_row_lineage_orc') WHERE _row_id = 1 SETTINGS use_iceberg_partition_pruning = 1;
SELECT i FROM icebergLocal('iceberg_v3/iceberg_v3_row_lineage_orc') WHERE _last_updated_sequence_number > 1 ORDER BY i SETTINGS use_iceberg_partition_pruning = 1;
"
