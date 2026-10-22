#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A chain of distinct messages, each having a field of the type of the next one, is not limited
# by the protobuf library, but was processed recursively both by the schema inference and while
# building the serializer, so a long chain used to exhaust the thread stack.

# Prints a schema with the messages `M0`, ..., `M<depth>`, where `M<i>` has the field `f` of the type `M<i+1>`,
# and the last one has the field `x` of the type `uint32`.
make_schema() {
    python3 -c "
depth = $1
print('syntax = \"proto3\"; ' + ' '.join('message M{}{{M{} f=1;}}'.format(i, i + 1) for i in range(depth)) + ' message M{}{{uint32 x=1;}}'.format(depth))
"
}

# Prints the name of a column matching the field `x` of the last message, like `f.f.f.x`.
make_column_name() {
    python3 -c "print('f.' * $1 + 'x')"
}

check_too_deep() {
    local output
    output=$(cat)
    if grep -q -F 'TOO_DEEP_RECURSION' <<< "$output"
    then
        echo "OK"
    else
        echo "FAIL: $(head -c 1000 <<< "$output" | tr '\n' ' ')"
    fi
}

run_desc() {
    ${CLICKHOUSE_LOCAL} --query "
    DESC format(Protobuf, '')
    SETTINGS format_schema_source = 'string', format_schema = '$(make_schema "$1")', format_schema_message_name = 'M0'
    "
}

run_write() {
    ${CLICKHOUSE_LOCAL} --query "
    SELECT toUInt32(1) AS \`$(make_column_name "$1")\`
    FORMAT Protobuf
    SETTINGS format_schema_source = 'string', format_schema = '$(make_schema "$1")', format_schema_message_name = 'M0'
    "
}

DEPTH=2000

# Schema inference.
run_desc 3
run_desc $DEPTH 2>&1 | check_too_deep

# Building the serializer for a column nested as deep as the chain.
run_write 3 | od -An -tx1 -v | tr -d ' \n'
echo
run_write $DEPTH 2>&1 | check_too_deep

# Unsupported fields skipped during schema inference must not count as nesting levels:
# a shallow message with many skipped fields of an empty message type.
SHALLOW_SCHEMA=$(python3 -c "
print('syntax = \"proto3\"; message E {} message Inner {' + ' '.join('E e{}={};'.format(i, i + 1) for i in range(1500)) + ' uint32 x=1501;} message Root {Inner m=1;}')
")
${CLICKHOUSE_LOCAL} --query "
DESC format(Protobuf, '')
SETTINGS format_schema_source = 'string', format_schema = '$SHALLOW_SCHEMA', format_schema_message_name = 'Root',
    input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference = 1
"
