#!/usr/bin/env bash
# Tags: no-fasttest

# Iceberg tables from the Apache Doris tests whose data files are outside the table location,
# see `data_iceberg/doris/README.md`. The layout of the MinIO bucket `warehouse` is kept:
# the metadata is in `warehouse/wh/test_db/<table>/metadata`, the data files are in `warehouse/data/...`.
# The expected results are taken from `regression-test/data/external_table_p0/iceberg/test_gen_iceberg_by_api.out`
# and `iceberg_read_unitycatalog_table.out`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `icebergLocal` in `clickhouse-local` only reads paths inside the current directory.
cd "$CUR_DIR/data_iceberg/doris" || exit 1

# Created with the Iceberg Java API: the table location is `s3a://warehouse/wh/test_db/multi_partition2`,
# four existing Parquet files `s3://warehouse/data/multi_partition2/*.parquet` are appended
# with the partition values (identity `id`, `hour(ts)`) given explicitly.
# The scheme of the data files (`s3://`) differs from the scheme of the table location (`s3a://`), and currently
# the path is mangled into `arehouse/data/multi_partition2/...`, so the query fails with FILE_DOESNT_EXIST
# (https://github.com/ClickHouse/ClickHouse/issues/129990).
echo "multi_partition2"
$CLICKHOUSE_LOCAL -q "
    SELECT id, toTimeZone(ts, 'Asia/Shanghai'), val FROM icebergLocal('warehouse/wh/test_db/multi_partition2') ORDER BY val;
    SELECT count() FROM icebergLocal('warehouse/wh/test_db/multi_partition2') WHERE id = 2;
" 2>&1

# A Delta Lake table with UniForm (Iceberg metadata written by Unity Catalog): the table location is
# `s3a://warehouse/wh/test_db/unitycatalog_marksheet_uniform`, the data file is
# `s3a://warehouse/data/unitycatalog/marksheet_uniform/part-00000-...snappy.parquet`.
# The Parquet columns have physical names of the Delta column mapping (`col-b1510e05-...`)
# and are matched by field ids.
echo "unitycatalog_marksheet_uniform"
$CLICKHOUSE_LOCAL -q "
    SELECT * FROM icebergLocal('warehouse/wh/test_db/unitycatalog_marksheet_uniform') ORDER BY c1;
    SELECT c1 FROM icebergLocal('warehouse/wh/test_db/unitycatalog_marksheet_uniform') WHERE c1 > 6 ORDER BY c1;
    SELECT c2, c3 FROM icebergLocal('warehouse/wh/test_db/unitycatalog_marksheet_uniform') WHERE c3 > 200 ORDER BY c1;
" 2>&1
