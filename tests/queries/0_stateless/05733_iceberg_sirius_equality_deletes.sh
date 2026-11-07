#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Equality delete files in Iceberg tables from the Sirius test suite, see `data_iceberg/sirius/README.md`.
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
-- Five rows apple/1 .. elderberry/5; an equality delete file keyed on (fruit, count) removes banana/2 and date/4.
SELECT 'iceberg_v2_equality_delete';
SELECT fruit, count FROM icebergLocal('$T/iceberg_v2_equality_delete') ORDER BY ALL;
SELECT count() FROM icebergLocal('$T/iceberg_v2_equality_delete');
SELECT fruit, count, _file, _row_number FROM icebergLocal('$T/iceberg_v2_equality_delete') WHERE count > 2 ORDER BY count;

-- The key is \`fruit\` alone (field id 1).
SELECT 'iceberg_v2_eq_single_col';
SELECT fruit, count FROM icebergLocal('$T/iceberg_v2_eq_single_col') ORDER BY ALL;

-- Two separate one-row equality delete files.
SELECT 'iceberg_v2_eq_multi_delete_file';
SELECT fruit, count FROM icebergLocal('$T/iceberg_v2_eq_multi_delete_file') ORDER BY ALL;

-- The delete file covers every data row.
SELECT 'iceberg_v2_eq_all_deleted';
SELECT fruit, count, _file, _row_number FROM icebergLocal('$T/iceberg_v2_eq_all_deleted');
SELECT count() FROM icebergLocal('$T/iceberg_v2_eq_all_deleted');

-- A positional delete removes row 0 (apple/1). The equality delete has the same sequence number as
-- the data file, so it does not apply: equality deletes only apply to strictly lower sequence numbers.
SELECT 'iceberg_v2_eq_pos_combined';
SELECT fruit, count, _file, _row_number FROM icebergLocal('$T/iceberg_v2_eq_pos_combined') ORDER BY ALL;

-- Two data files, one delete file removing a row from each (banana/2 and elderberry/5).
SELECT 'iceberg_v2_eq_multi_data_file';
SELECT fruit, count FROM icebergLocal('$T/iceberg_v2_eq_multi_data_file') ORDER BY ALL;

-- seq 1: apple/1, banana/2, cherry/3; seq 2: delete banana; seq 3: banana/4, date/5.
-- The re-inserted banana/4 has a higher sequence number than the delete and survives.
SELECT 'iceberg_eq_seq_number';
SELECT fruit, count FROM icebergLocal('$T/iceberg_eq_seq_number') ORDER BY count;

-- The only equality delete entry has status DELETED: nothing is deleted.
SELECT 'iceberg_v2_eq_delete_retired';
SELECT fruit, count FROM icebergLocal('$T/iceberg_v2_eq_delete_retired') ORDER BY ALL;
"
