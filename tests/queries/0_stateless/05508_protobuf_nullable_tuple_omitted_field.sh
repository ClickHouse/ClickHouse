#!/usr/bin/env bash
# Tags: no-fasttest

# A `Nullable(Tuple(...))` column mapped to a nested message that is absent in some rows.
# The default (`NULL`) is inserted into the tuple whose element columns are also held by the
# serializers of the nested message fields. That must not trip the exclusive-ownership assertion
# of `WrappedPtr` in debug and sanitizer builds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

SCHEMA="$CUR_DIR/format_schemas/05508_protobuf_nullable_tuple_omitted_field.proto:Row"
DATA_FILE="$CLICKHOUSE_TMP/05508_protobuf_nullable_tuple_omitted_field.bin"
trap 'rm -f "$DATA_FILE"' EXIT

COLUMNS="id UInt32, t Nullable(Tuple(x UInt32, a Array(UInt32)))"

echo "The nested message is absent in every row:"
$CLICKHOUSE_LOCAL --query "
    SELECT number + 1 AS id FROM numbers(3)
    INTO OUTFILE '$DATA_FILE' TRUNCATE
    FORMAT Protobuf SETTINGS format_schema = '$SCHEMA'"

$CLICKHOUSE_LOCAL --query "
    SELECT * FROM file('$DATA_FILE', 'Protobuf', '$COLUMNS')
    SETTINGS format_schema = '$SCHEMA'"

echo "The nested message is absent in some rows:"
$CLICKHOUSE_LOCAL --query "
    SELECT * FROM values('$COLUMNS', (1, NULL), (2, (5, [1, 2])), (3, NULL), (4, (6, [])))
    INTO OUTFILE '$DATA_FILE' TRUNCATE
    FORMAT Protobuf SETTINGS format_schema = '$SCHEMA'"

$CLICKHOUSE_LOCAL --query "
    SELECT * FROM file('$DATA_FILE', 'Protobuf', '$COLUMNS')
    SETTINGS format_schema = '$SCHEMA'"
