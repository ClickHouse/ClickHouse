#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Format version 3 Iceberg tables with deletion vectors and with row lineage, written by Spark, from the Apache Impala
# test data, see `data_iceberg/impala/README.md`. The expected results are the ones of the Impala tests
# `iceberg-v3-delete-existing-dv.test`, `iceberg-v3-optimize-shared-puffin.test` and `iceberg-v3-row-lineage.test`.
#
# For ORC data files, `_row_id` is currently NULL and `_last_updated_sequence_number` stored in the data file is ignored:
# https://github.com/ClickHouse/ClickHouse/issues/130032

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/impala" || exit 1

$CLICKHOUSE_LOCAL --session_timezone UTC --ignore-error -q "
-- Deletion vectors in Puffin files.
SELECT 'iceberg_v3_deletion_vectors';
SELECT * FROM icebergLocal('iceberg_v3/iceberg_v3_deletion_vectors') ORDER BY i;
SELECT count() FROM icebergLocal('iceberg_v3/iceberg_v3_deletion_vectors') SETTINGS optimize_count_from_files = 0;

-- One Puffin file with the deletion vectors of data files in two partitions.
SELECT 'iceberg_v3_dv_multi_partition';
SELECT * FROM icebergLocal('iceberg_v3/iceberg_v3_dv_multi_partition') ORDER BY ALL;
SELECT * FROM icebergLocal('iceberg_v3/iceberg_v3_dv_multi_partition') WHERE p = 2;

-- Row lineage: three appends of one row each, then a compaction that stores _row_id and _last_updated_sequence_number
-- in the new data file. Before the compaction, the values are inherited from the manifests.
SELECT 'iceberg_v3_row_lineage';
SELECT * FROM icebergLocal('iceberg_v3/iceberg_v3_row_lineage') ORDER BY i;
SELECT i, _row_id, _last_updated_sequence_number FROM icebergLocal('iceberg_v3/iceberg_v3_row_lineage') ORDER BY i;
SELECT i, _row_id, _last_updated_sequence_number FROM icebergLocal('iceberg_v3/iceberg_v3_row_lineage') ORDER BY i SETTINGS iceberg_snapshot_id = 2872597867664652808;
SELECT 'iceberg_v3_row_lineage_orc';
SELECT * FROM icebergLocal('iceberg_v3/iceberg_v3_row_lineage_orc') ORDER BY i;
SELECT i, _row_id, _last_updated_sequence_number FROM icebergLocal('iceberg_v3/iceberg_v3_row_lineage_orc') ORDER BY i;
SELECT i, _row_id, _last_updated_sequence_number FROM icebergLocal('iceberg_v3/iceberg_v3_row_lineage_orc') ORDER BY i SETTINGS iceberg_snapshot_id = 7033898671372067760;
" 2>&1
