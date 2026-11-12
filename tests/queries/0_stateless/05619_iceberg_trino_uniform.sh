#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg and Avro are not in fast test

# Delta Lake tables written by Databricks 13.3 and 14.3 with UniForm (`delta.universalFormat.enabledFormats`
# = `iceberg`, IcebergCompatV1 and V2), read through their Iceberg metadata. The tables are shared with
# `05544_delta_lake_trino_table_features`, see `data_delta_lake/trino/README.md`; the expected result
# comes from the Trino tests `TestDeltaLakeBasic.testUniFormIcebergV1` and `testUniFormIcebergV2`.
# The Iceberg metadata refers to the files with the original `s3://trino-ci-test/databricks-unity/...` location.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `icebergLocal` refuses a path outside the working directory of `clickhouse local`.
cd "$CUR_DIR/data_delta_lake/trino" || exit 1

$CLICKHOUSE_LOCAL -q "
SELECT 'databricks133/uniform_iceberg_v1';
DESCRIBE icebergLocal('databricks133/uniform_iceberg_v1');
SELECT * FROM icebergLocal('databricks133/uniform_iceberg_v1');
SELECT 'databricks143/uniform_iceberg_v2';
DESCRIBE icebergLocal('databricks143/uniform_iceberg_v2');
SELECT * FROM icebergLocal('databricks143/uniform_iceberg_v2');
"
