#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Schema evolution in Iceberg tables from the Sirius test suite, see `data_iceberg/sirius/README.md`.
# The expected results are taken from the Sirius C++ integration tests
# (`test/cpp/integration/test_gpu_execution_multi_format.cpp`).
# The metadata stores paths relative to the Sirius repository root (`test/cpp/...`),
# so the queries run with `data_iceberg/sirius` as the current directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

cd "$CUR_DIR/data_iceberg/sirius" || exit 1
T="test/cpp/integration/data"
C="$T/iceberg_conformance"

$CLICKHOUSE_LOCAL -q "
-- Tables written by PyIceberg; the expected rows are from \`iceberg_conformance/expectations.json\`,
-- recorded by PyIceberg's own scan.
SELECT 'conformance/append_only';
SELECT id, name FROM icebergLocal('$C/append_only/conf/append_only') ORDER BY ALL;

-- \`y\` was dropped and re-added under the same name, so it is a new field id (4). The only data file
-- holds the original \`y\` (field id 3), so the current \`y\` reads NULL.
SELECT 'conformance/drop_readd';
SELECT id, x, y FROM icebergLocal('$C/drop_readd/conf/drop_readd') ORDER BY ALL;
-- The only snapshot was written with schema 0, where \`y\` is still field id 3: time travel to it
-- returns the old values.
SELECT id, x, y FROM icebergLocal('$C/drop_readd/conf/drop_readd') ORDER BY ALL SETTINGS iceberg_snapshot_id = 2926213281689314888;

-- A renamed column keeps its field id; the old data file carries the original name.
SELECT 'conformance/rename_col';
SELECT id, value FROM icebergLocal('$C/rename_col/conf/rename_col') ORDER BY ALL;

-- A column added after the first data file was written reads NULL for that file.
SELECT 'conformance/add_column';
SELECT id, a, b FROM icebergLocal('$C/add_column/conf/add_column') ORDER BY ALL;

-- Hand-forged: the second snapshot added \`color\`, absent from the first data file.
SELECT 'iceberg_schema_evolution';
SELECT * FROM icebergLocal('$T/iceberg_schema_evolution') ORDER BY ALL;
SELECT * FROM icebergLocal('$T/iceberg_schema_evolution') ORDER BY ALL SETTINGS iceberg_snapshot_id = 9100000000000001;
"

# Format version 3 table with evolved fields and a live positional delete file. The data file
# holds `fruit` (id 1), `count` as INT32 (id 2) and `old_score` as FLOAT32 (id 4). The table schema
# renames `old_score` to `score` and promotes it to `double`, promotes `count` to `long`, has a new
# `fruit` with field id 3 (absent from the file, so NULL) and a new column `added` with
# `initial-default` 7 (and `write-default` 9), which must be read as 7 for the existing rows.
# Bug: `initial-default` is not applied, `added` is read as NULL
# (https://github.com/ClickHouse/ClickHouse/issues/129992).
$CLICKHOUSE_LOCAL -q "
SELECT 'iceberg_evolved_fields_with_deletes';
SELECT * FROM icebergLocal('$T/iceberg_evolved_fields_with_deletes') ORDER BY count;
SELECT count, score, fruit, added, _row_number FROM icebergLocal('$T/iceberg_evolved_fields_with_deletes') WHERE count >= 3 ORDER BY count;
SELECT count() FROM icebergLocal('$T/iceberg_evolved_fields_with_deletes');
SELECT count() FROM icebergLocal('$T/iceberg_evolved_fields_with_deletes') WHERE score IS NULL;
SELECT count() FROM icebergLocal('$T/iceberg_evolved_fields_with_deletes') WHERE score IS NOT NULL AND count IS NOT NULL;
SELECT count() FROM icebergLocal('$T/iceberg_evolved_fields_with_deletes') WHERE added = 7;
"
