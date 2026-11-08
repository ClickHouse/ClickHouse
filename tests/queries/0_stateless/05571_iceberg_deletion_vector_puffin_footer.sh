#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg is not in fast test

# A deletion vector applies to the data file that the Puffin footer names as the `referenced-data-file` of its blob
# (https://github.com/ClickHouse/ClickHouse/issues/129996). Tables, copied without modification:
# - data_iceberg/sirius: https://github.com/sirius-db/sirius/tree/eb9c3a132d3dcd00d8a480852534f3a3b8cf490c/test/cpp/integration/data (Apache License 2.0)
# - data_iceberg/duckdb: https://github.com/duckdb/duckdb-iceberg/tree/b57f45c2c78965c71f699aa23ae73c292923668e/data/persistent (MIT License, Copyright 2018-2025 Stichting DuckDB Foundation)
# - data_iceberg/tileverse: https://github.com/tileverse-io/parquetry/tree/fba9d4db27f93fd7e21f2dd121bd81f40646347e/internal/parquetry-testkit/src/main/resources (Apache License 2.0)
# The tables in data_iceberg/dv_puffin_footer are made from the Sirius tables by data_iceberg/dv_puffin_footer/generate.py.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function expect_violation()
{
    $CLICKHOUSE_LOCAL -q "$1" 2>&1 | grep -o -m1 'ICEBERG_SPECIFICATION_VIOLATION'
}

cd "$CUR_DIR/data_iceberg/sirius" || exit 1
T="test/cpp/integration/data"

# The manifest entries of the two deletion vectors carry each other's `content_offset`.
echo 'iceberg_v3_dv_misbound'
expect_violation "SELECT fruit, count FROM icebergLocal('$T/iceberg_v3_dv_misbound') ORDER BY ALL"
expect_violation "SELECT count() FROM icebergLocal('$T/iceberg_v3_dv_misbound')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM icebergLocal('$T/iceberg_v3_dv_misbound')" 2>&1 | grep -c "referenced-data-file. is '$T/iceberg_v3_dv_misbound/data/0000[01]-"

echo 'iceberg_v3_deletion_vector'
$CLICKHOUSE_LOCAL -q "SELECT fruit, count FROM icebergLocal('$T/iceberg_v3_deletion_vector') ORDER BY ALL"

# A deletion vector written by old DuckDB versions is a whole file without a Puffin footer.
cd "$CUR_DIR/data_iceberg/duckdb" || exit 1
echo 'legacy_bare_deletion_vector'
$CLICKHOUSE_LOCAL -q "SELECT id, source FROM icebergLocal('data/persistent/legacy_bare_deletion_vector/warehouse/default/legacy_bare_deletion_vector') ORDER BY id"

# The table was relocated without rewriting the Puffin footer, which keeps the old path of the data file.
cd "$CUR_DIR/data_iceberg/tileverse" || exit 1
echo 'deletion-vectors'
$CLICKHOUSE_LOCAL -q "SELECT count(), sum(id) FROM icebergLocal('iceberg-deletes/deletion-vectors')"

cd "$CUR_DIR/data_iceberg/dv_puffin_footer" || exit 1

echo 'unknown_range'
expect_violation "SELECT fruit, count FROM icebergLocal('unknown_range') ORDER BY ALL"

echo 'same_name_swapped'
expect_violation "SELECT fruit, count FROM icebergLocal('same_name_swapped') ORDER BY ALL"

# A blob of another type in the same Puffin file is ignored.
echo 'mixed_blobs'
$CLICKHOUSE_LOCAL -q "SELECT fruit, count FROM icebergLocal('mixed_blobs') ORDER BY ALL"

echo 'duplicate_range'
expect_violation "SELECT fruit, count FROM icebergLocal('duplicate_range') ORDER BY ALL"
