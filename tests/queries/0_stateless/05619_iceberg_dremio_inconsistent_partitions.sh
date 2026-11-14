#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `icebergLocal` needs the USE_AVRO build option.

# A table from the Dremio test suite (`TestIcebergManifestListRecordReader.testInconsistentPartitionDepth`),
# see `data_iceberg/dremio/README.md`. It is partitioned by identity of `dir0` and `dir1`, the data files contain only
# the other columns. One of the three manifests has a partition tuple with `dir0` only, and the manifests have no
# `partition-spec` and `schema` keys in their Avro metadata. The missing `dir1` is NULL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/dremio"

q()
{
    $CLICKHOUSE_LOCAL --session_timezone UTC --query "$1"
}

q "DESCRIBE icebergLocal('$DATA/internal_metadata_inconsistent_partitions') SETTINGS describe_compact_output = 1"
q "SELECT count() FROM icebergLocal('$DATA/internal_metadata_inconsistent_partitions')"
q "SELECT * FROM icebergLocal('$DATA/internal_metadata_inconsistent_partitions') ORDER BY ALL"

# Partition pruning on the column that is missing in one of the partition tuples.
q "SELECT * FROM icebergLocal('$DATA/internal_metadata_inconsistent_partitions') WHERE dir1 = 'sub_1_1' ORDER BY ALL"
q "SELECT * FROM icebergLocal('$DATA/internal_metadata_inconsistent_partitions') WHERE dir1 IS NULL ORDER BY ALL"
q "SELECT * EXCEPT (dir0, dir1) FROM icebergLocal('$DATA/internal_metadata_inconsistent_partitions') WHERE dir0 = 'sub_2' ORDER BY ALL"
