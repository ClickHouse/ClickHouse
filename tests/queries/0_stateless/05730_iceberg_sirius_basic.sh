#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# Append-only Iceberg tables hand-forged by the Sirius test suite, see `data_iceberg/sirius/README.md`.
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
-- A format version 1 table with one data file.
SELECT 'iceberg_v1';
SELECT * FROM icebergLocal('$T/iceberg_v1') ORDER BY ALL;
SELECT count() FROM icebergLocal('$T/iceberg_v1');
SELECT fruit, count FROM icebergLocal('$T/iceberg_v1') WHERE count > 1 ORDER BY count;

-- Manifests compressed with the Avro \`deflate\` codec.
SELECT 'iceberg_deflate_manifest';
SELECT * FROM icebergLocal('$T/iceberg_deflate_manifest') ORDER BY ALL;

-- The data file stores \`count\` as INT32 while the table declares it \`long\`.
SELECT 'iceberg_v1_promoted_type';
SELECT * FROM icebergLocal('$T/iceberg_v1_promoted_type') ORDER BY ALL;
SELECT toTypeName(count) FROM icebergLocal('$T/iceberg_v1_promoted_type') LIMIT 1;

-- The data file stores the columns in the opposite physical order; they are matched by field id.
SELECT 'iceberg_v1_reordered';
SELECT fruit, count FROM icebergLocal('$T/iceberg_v1_reordered') ORDER BY ALL;

-- The data file has no Parquet field ids; \`schema.name-mapping.default\` maps the columns.
SELECT 'iceberg_v1_no_field_ids';
SELECT fruit, count FROM icebergLocal('$T/iceberg_v1_no_field_ids') ORDER BY ALL;

-- An identity partition column that is absent from the data file is filled from the partition value.
SELECT 'iceberg_missing_partition_column';
SELECT * FROM icebergLocal('$T/iceberg_missing_partition_column') ORDER BY ALL;
"
