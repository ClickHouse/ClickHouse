#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: ORC is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA="$CUR_DIR/data_orc"
FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.orc"
trap 'rm -f "$FILE"' EXIT

# `iceberg_uuid.orc` is a data file of the Iceberg table `iceberg_uuid_test_orc` from the Apache Impala test data.
# `iceberg_uuid_nested.orc` was written by pyorc, with the attribute `iceberg.binary-type=UUID` on its `binary` nodes.
$CLICKHOUSE_LOCAL -q "
    SELECT 'nullable';
    SELECT id, uuid_col, name FROM file('$DATA/iceberg_uuid.orc', ORC, 'id Nullable(Int32), uuid_col Nullable(UUID), name Nullable(String)') ORDER BY id;
    SELECT 'not nullable';
    SELECT id, uuid_col FROM file('$DATA/iceberg_uuid.orc', ORC, 'id Int32, uuid_col UUID, name String') ORDER BY id;
    SELECT 'low cardinality';
    SELECT id, uuid_col FROM file('$DATA/iceberg_uuid.orc', ORC, 'id Nullable(Int32), uuid_col LowCardinality(Nullable(UUID)), name Nullable(String)') ORDER BY id;
    SELECT id, uuid_col FROM file('$DATA/iceberg_uuid.orc', ORC, 'id Int32, uuid_col LowCardinality(UUID), name String') ORDER BY id;
    SELECT id, l FROM file('$DATA/iceberg_uuid_nested.orc', ORC, 'id Int32, l Array(LowCardinality(Nullable(UUID)))') ORDER BY id;
    SELECT 'filter';
    SELECT id FROM file('$DATA/iceberg_uuid.orc', ORC, 'id Nullable(Int32), uuid_col Nullable(UUID), name Nullable(String)')
        WHERE uuid_col = 'abcdefab-cdef-abcd-efab-cdefabcdefab' SETTINGS input_format_orc_filter_push_down = 1;
    SELECT 'nested';
    SELECT id, l, m, s FROM file('$DATA/iceberg_uuid_nested.orc', ORC, 'id Int32, l Array(Nullable(UUID)), m Map(String, Nullable(UUID)), s Tuple(x Nullable(UUID))') ORDER BY id;
    SELECT 'union';
    SELECT id, v, variantType(v) FROM file('$DATA/iceberg_uuid_nested.orc', ORC, 'id Int32, v Variant(String, UUID)') ORDER BY id;
    SELECT id, v, variantType(v) FROM file('$DATA/iceberg_uuid_nested.orc', ORC, 'id Int32, v Variant(String, UUID, IPv6)') ORDER BY id;
    SELECT 'inferred';
    DESCRIBE file('$DATA/iceberg_uuid.orc', ORC) SETTINGS describe_compact_output = 1;
    DESCRIBE file('$DATA/iceberg_uuid_nested.orc', ORC) SETTINGS describe_compact_output = 1;
    SELECT * FROM file('$DATA/iceberg_uuid.orc', ORC) ORDER BY id;
"

echo 'wrong length'
$CLICKHOUSE_LOCAL -q "SELECT bad FROM file('$DATA/iceberg_uuid_nested.orc', ORC, 'bad Nullable(UUID)')" 2>&1 | grep -o -m1 'INCORRECT_DATA'

echo 'binary without the attribute'
$CLICKHOUSE_LOCAL -q "SELECT '00112233-4455-6677-8899-aabbccddeeff' AS u INTO OUTFILE '$FILE' TRUNCATE FORMAT ORC SETTINGS output_format_orc_string_as_string = 0"
$CLICKHOUSE_LOCAL -q "SELECT u FROM file('$FILE', ORC, 'u UUID')"
