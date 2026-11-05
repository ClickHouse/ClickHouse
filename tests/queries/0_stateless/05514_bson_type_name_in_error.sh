#!/usr/bin/env bash
# `getBSONTypeName` (src/Formats/BSONTypes.cpp) names a BSON type in the error that
# BSONEachRow input reports when a field cannot go into the target column. Its switch has
# no `default:` and no return after the switch, so a type accepted by `getBSONType` but
# missing a case would fall off the end of the function. Every type byte that
# `getBSONType` accepts (0x01..0x13, 0x7F, 0xFF) is therefore checked below; the type
# names are the ones from https://bsonspec.org/spec.html.
#
# `readAndInsertUUID` rejects a field on its type byte alone, before any value byte is
# read, so one minimal 8-byte document is enough for every type:
#   08 00 00 00   document size = 8
#   <type>        the type byte under test
#   75 00         key "u\0"
#   00            document terminator
# Both the error code name and the BSON type name are read back out of the reported
# error, so a wrong name for any of these types changes the result of the test. The eleven
# rejected types are sent over HTTP, which reports the error in the response body, so the
# loop needs no client process per type.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 0x06 Undefined, 0x07 ObjectId, 0x0B Regexp, 0x0C DBPointer, 0x0D JavaScript code,
# 0x0E Symbol, 0x0F JavaScript code w/ scope, 0x11 Timestamp, 0x13 Decimal128,
# 0x7F Max key, 0xFF Min key.
for type_byte in 06 07 0b 0c 0d 0e 0f 11 13 7f ff
do
    printf '0x%s ' "$type_byte"
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}" \
        -d "SELECT * FROM format(BSONEachRow, 'u UUID', x'08000000${type_byte}750000')" \
        | sed -n 's/.*Cannot insert BSON \(.*\) into UUID column.*(\([A-Z_]*\)).*/\2 \1/p' | head -n 1
done

# The remaining type bytes 0x01..0x05, 0x08..0x0A, 0x10 and 0x12 are already named by
# 04112_bson_nullable_array_read_rollback and skipped by
# 05359_bson_skip_unknown_field_types. 0x0A Null is the one name no input can ask for:
# `BSONEachRowRowInputFormat::readField` handles NULL_VALUE before the type switch and
# schema inference maps it to no type at all.

# Three of the type bytes above are accepted by a String column and decode to a value,
# which fixes what those bytes mean: 0x0E and 0x0D carry a length-prefixed string, 0x07
# carries twelve raw bytes. Reading one byte too few or too many would miss the document
# terminator, so the decoded value is what proves the encoding.

# Symbol: size 15 | 0E | key "s" | length 3 | "ab" + NUL | terminator
$CLICKHOUSE_CLIENT --query "SELECT count(), max(s) FROM format(BSONEachRow, 's String', x'0f0000000e73000300000061620000')"
# JavaScript code: size 15 | 0D | key "s" | length 3 | "cd" + NUL | terminator
$CLICKHOUSE_CLIENT --query "SELECT count(), max(s) FROM format(BSONEachRow, 's String', x'0f0000000d73000300000063640000')"
# ObjectId: size 20 | 07 | key "s" | twelve bytes | terminator
$CLICKHOUSE_CLIENT --query "SELECT count(), hex(max(s)) FROM format(BSONEachRow, 's String', x'14000000077300000102030405060708090a0b00')"
