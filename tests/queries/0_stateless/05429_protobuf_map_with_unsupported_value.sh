#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `map` whose value type is unsupported is skipped in schema inference with
# `input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference = 1`.

SCHEMADIR="$CUR_DIR/format_schemas"
$CLICKHOUSE_LOCAL -q "DESC file('nonexist', 'Protobuf') SETTINGS format_schema = '$SCHEMADIR/05429_protobuf_map_with_unsupported_value.proto:MessageWithMaps', input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference = 0" 2>&1 | grep -o -m1 -F 'BAD_ARGUMENTS'
$CLICKHOUSE_LOCAL -q "DESC file('nonexist', 'Protobuf') SETTINGS format_schema = '$SCHEMADIR/05429_protobuf_map_with_unsupported_value.proto:MessageWithMaps', input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference = 1"
