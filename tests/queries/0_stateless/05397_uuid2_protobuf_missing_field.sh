#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: Protobuf support is not compiled into the fast-test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `UUID` or `UUID2` column mapped to a Protobuf field that is absent from the message gets the nil UUID
# when the schema declares no default, and the declared default otherwise.

SCHEMA_DIR=test_05397_$CLICKHOUSE_DATABASE
SERVER_SCHEMADIR=$CLICKHOUSE_SCHEMA_FILES/$SCHEMA_DIR
mkdir -p "$SERVER_SCHEMADIR"
cp "$CUR_DIR"/format_schemas/05397_uuid2_protobuf_missing_field.proto "$SERVER_SCHEMADIR"/
DATA_FILE=05397_uuid2_protobuf_${CLICKHOUSE_DATABASE}.bin
FORMAT_SCHEMA="$SCHEMA_DIR/05397_uuid2_protobuf_missing_field.proto:Row"

# Write a message containing only `n`.
$CLICKHOUSE_CLIENT -q "INSERT INTO FUNCTION file('$DATA_FILE', 'Protobuf', 'n Int32') SELECT 1 SETTINGS format_schema = '$FORMAT_SCHEMA', engine_file_truncate_on_insert = 1"

for type in UUID UUID2
do
    $CLICKHOUSE_CLIENT -q "SELECT toTypeName(u), toString(u), toString(d), n FROM file('$DATA_FILE', 'Protobuf', 'u $type, d $type, n Int32') SETTINGS format_schema = '$FORMAT_SCHEMA'"
done

rm "$CLICKHOUSE_USER_FILES/$DATA_FILE"
rm -r "$SERVER_SCHEMADIR"
