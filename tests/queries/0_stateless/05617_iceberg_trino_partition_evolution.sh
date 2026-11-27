#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg and Avro are not in fast test

# An Iceberg table written by Trino 476 whose partitioning evolved from `truncate(a, 1)` to
# `truncate(a, 10)` on the same column, so that both partition specs have a field named `a_trunc`
# (with different field ids). The table and the expected results come from the Trino test
# `TestIcebergPartitionEvolutionOnSameColumn.testFilesPartitionEvolutionWithDuplicatePartitionFieldNames`,
# see `data_iceberg/trino/README.md`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_iceberg/trino"

# `icebergLocal` refuses a path outside the working directory of `clickhouse local`.
cd "$DATA" || exit 1

$CLICKHOUSE_LOCAL -q "
DESCRIBE icebergLocal('conflict_truncate');
SELECT * FROM icebergLocal('conflict_truncate') ORDER BY ALL;
-- One data file per partition spec.
SELECT uniqExact(_path) FROM icebergLocal('conflict_truncate');

-- Partition pruning must use the transform of the spec of each data file.
SELECT a FROM icebergLocal('conflict_truncate') WHERE a = 'abc';
SELECT a FROM icebergLocal('conflict_truncate') WHERE a = 'abcd';
SELECT a FROM icebergLocal('conflict_truncate') WHERE a LIKE 'abc%' ORDER BY a;
SELECT count() FROM icebergLocal('conflict_truncate') WHERE a = 'b';

-- The snapshot of the first insert, before the partitioning was changed (\`total-records\` = 1 in its summary).
SELECT * FROM icebergLocal('conflict_truncate') SETTINGS iceberg_snapshot_id = 1176079920173818583;
"
