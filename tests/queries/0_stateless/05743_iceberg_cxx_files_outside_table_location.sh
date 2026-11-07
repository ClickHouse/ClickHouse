#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Iceberg and Parquet support.

# Iceberg tables from lithium-tech/iceberg-cxx and lithium-tech/tea whose files are outside the table
# location, see `data_iceberg/iceberg_cxx/README.md` and `data_iceberg/tea/README.md`. The paths outside
# the table location must be resolved relative to the common parent directory of the table and the file.
# Bug: https://github.com/ClickHouse/ClickHouse/issues/129990 (`IcebergPathResolver::resolve` mangles them).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# clickhouse-local allows `icebergLocal` only for paths inside the current directory.
cd "$CUR_DIR/data_iceberg" || exit 1

# The table location is `s3a://dl-test-maintenance/check/tea-partitioned/v_20240913/iceberg`, and the data
# files, partitioned by day(timestamptzcol), are in `.../v_20240913/timestamptzcol_day=.../` next to it.
# Two of the three data files are empty. The element of `intarrcol` has field id 500 in the data file but
# 13 in the table schema, so the column is left out: readers disagree on it (PyIceberg returns NULL).
# Expected values: PyIceberg and the data file itself.
$CLICKHOUSE_LOCAL --session_timezone UTC -q "
SELECT 'iceberg_cxx/v_20240913/iceberg';
SELECT * EXCEPT intarrcol FROM icebergLocal('iceberg_cxx/v_20240913/iceberg');
SELECT count() FROM icebergLocal('iceberg_cxx/v_20240913/iceberg') WHERE timestamptzcol >= '2024-09-13 00:00:00';
SELECT count() FROM icebergLocal('iceberg_cxx/v_20240913/iceberg') WHERE timestamptzcol IS NULL;
"

# The table location is `s3a://warehouse/unsupported_historical_schema`, but the manifest lists are in the
# sibling table `s3a://warehouse/snapshot_selection` (also included). The old snapshot has an older schema,
# where c1 is a list of structs, and no rows (upstream test `UnsupportedHistoricalSchemaLatestWorks`).
$CLICKHOUSE_LOCAL -q "
SELECT 'tea/unsupported_historical_schema';
SELECT * FROM icebergLocal('tea/unsupported_historical_schema') ORDER BY ALL;
SELECT count() FROM icebergLocal('tea/unsupported_historical_schema') SETTINGS iceberg_snapshot_id = 2425900280988415891;
"
