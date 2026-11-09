#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Positional delete files in Iceberg tables from the Sirius test suite, see `data_iceberg/sirius/README.md`.
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
-- Five rows apple/1 .. elderberry/5; the positional delete file removes positions 1 and 3 (banana/2, date/4).
SELECT 'iceberg_v2_delete';
SELECT fruit, count FROM icebergLocal('$T/iceberg_v2_delete') ORDER BY ALL;
SELECT count() FROM icebergLocal('$T/iceberg_v2_delete');
SELECT fruit, count FROM icebergLocal('$T/iceberg_v2_delete') ORDER BY count DESC;
-- The surviving rows keep their original positions in the data file.
SELECT fruit, count, _file, _row_number FROM icebergLocal('$T/iceberg_v2_delete') ORDER BY ALL;

-- The current snapshot adds the delete file; the first snapshot predates it.
SELECT 'iceberg_snapshot_deletes';
SELECT * FROM icebergLocal('$T/iceberg_snapshot_deletes') ORDER BY ALL;
SELECT sum(count) FROM icebergLocal('$T/iceberg_snapshot_deletes');
SELECT * FROM icebergLocal('$T/iceberg_snapshot_deletes') ORDER BY ALL SETTINGS iceberg_snapshot_id = 9400000000000001;
SELECT * FROM icebergLocal('$T/iceberg_snapshot_deletes') ORDER BY ALL SETTINGS iceberg_snapshot_id = 9400000000000002;

-- The manifest still lists the positional delete file, but with status DELETED: it no longer applies.
SELECT 'iceberg_v2_pos_delete_retired';
SELECT fruit, count FROM icebergLocal('$T/iceberg_v2_pos_delete_retired') ORDER BY ALL;
SELECT count() FROM icebergLocal('$T/iceberg_v2_pos_delete_retired');
"
