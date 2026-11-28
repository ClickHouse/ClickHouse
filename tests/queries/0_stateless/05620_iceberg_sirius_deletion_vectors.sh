#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Deletion vectors (format version 3) in Iceberg tables from the Sirius test suite, see `data_iceberg/sirius/README.md`.
# The expected results are taken from the Sirius C++ integration tests
# (`test/cpp/integration/test_gpu_execution_multi_format.cpp`).
# The metadata stores paths relative to the Sirius repository root (`test/cpp/...`),
# so the queries run with `data_iceberg/sirius` as the current directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/sirius" || exit 1
T="test/cpp/integration/data"

$CLICKHOUSE_LOCAL -q "
-- Same data as \`iceberg_v2_delete\`; a deletion vector in a Puffin file removes positions 1 and 3.
SELECT 'iceberg_v3_deletion_vector';
SELECT fruit, count FROM icebergLocal('$T/iceberg_v3_deletion_vector') ORDER BY ALL;
SELECT count() FROM icebergLocal('$T/iceberg_v3_deletion_vector');
SELECT fruit, count, _file, _row_number FROM icebergLocal('$T/iceberg_v3_deletion_vector') WHERE count > 2 ORDER BY count;

-- Two deletion vectors for one data file: the live replacement (ADDED, deletes positions 1 and 3)
-- and the vector it replaced (DELETED, deletes positions 0, 1, 2 and 4), written last.
SELECT 'iceberg_v3_dv_replaced';
SELECT fruit, count FROM icebergLocal('$T/iceberg_v3_dv_replaced') ORDER BY ALL;
"

# One Puffin file holds a deletion vector for each of two data files, and the two manifest entries
# carry each other's `content_offset`. Only the `referenced-data-file` of the blobs in the Puffin
# footer reveals the swap; the Iceberg spec requires the offset and size to match the footer, and
# Sirius expects the table to be refused.
# Bug: the vectors are applied to the wrong data files and 8 wrong rows are returned
# (https://github.com/ClickHouse/ClickHouse/issues/129996).
echo 'iceberg_v3_dv_misbound'
$CLICKHOUSE_LOCAL -q "SELECT fruit, count FROM icebergLocal('$T/iceberg_v3_dv_misbound') ORDER BY ALL" 2>&1 | grep -o -m1 'ICEBERG_SPECIFICATION_VIOLATION'
$CLICKHOUSE_LOCAL -q "SELECT count() FROM icebergLocal('$T/iceberg_v3_dv_misbound')" 2>&1 | grep -o -m1 'ICEBERG_SPECIFICATION_VIOLATION'
