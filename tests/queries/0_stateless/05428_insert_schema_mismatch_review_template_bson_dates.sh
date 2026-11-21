#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Each case below has exactly one invalid value, which produces the genuine parse error that triggers
# the schema-mismatch diagnostic, beside a valid value that schema inference cannot represent exactly.
# The valid value must not attract the "structure does not match" explanation.

PHRASE="does not match the structure expected by the query"

check() {
    if grep -q "$PHRASE"; then echo "explanation present"; else echo "explanation missing"; fi
}

echo "-- Template: the parser maps placeholders by name, so a reordered row format is valid"
ROW_REORDERED="$CLICKHOUSE_TMP/05428_row_reordered.fmt"
printf '${b:CSV}|${a:CSV}' > "$ROW_REORDERED"
printf "hello|1.5" | $CLICKHOUSE_LOCAL --format_template_row="$ROW_REORDERED" \
    --query "CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT Template" 2>&1 | check
printf "hello|1" | $CLICKHOUSE_LOCAL --format_template_row="$ROW_REORDERED" \
    --query "CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT Template" 2>&1 && echo "inserted"

echo "-- Template: a destination column omitted from the row format is filled with defaults"
ROW_PARTIAL="$CLICKHOUSE_TMP/05428_row_partial.fmt"
printf '${a:CSV}' > "$ROW_PARTIAL"
printf "1.5" | $CLICKHOUSE_LOCAL --format_template_row="$ROW_PARTIAL" \
    --query "CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT Template" 2>&1 | check
printf "1" | $CLICKHOUSE_LOCAL --format_template_row="$ROW_PARTIAL" \
    --query "CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT Template" 2>&1 && echo "inserted"

echo "-- Template: a genuine mismatch in a named placeholder is still reported"
printf "x" | $CLICKHOUSE_LOCAL --format_template_row="$ROW_PARTIAL" \
    --query "CREATE TABLE t (a UInt8, b String) ENGINE = Memory; INSERT INTO t FORMAT Template" 2>&1 | check

rm -f "$ROW_REORDERED" "$ROW_PARTIAL"

# The field `u` is a BSON binary with the UUID subtype but only 4 bytes of payload, which fails with a
# genuine parse error (wrong binary size for a `UUID`).
echo "-- BSONEachRow: a BSON array is a valid value for an unnamed Tuple column"
{
    echo "CREATE TABLE t (u UUID, a Tuple(UInt8, UInt8)) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), a: [int32 1, int32 2]}
    printf '\x27\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x04a\x00\x13\x00\x00\x00\x100\x00\x01\x00\x00\x00\x101\x00\x02\x00\x00\x00\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- BSONEachRow: a BSON document is a valid value for a named Tuple column"
{
    echo "CREATE TABLE t (u UUID, a Tuple(x UInt8, y UInt8)) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), a: {x: int32 1, y: int32 2}}
    printf '\x27\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x03a\x00\x13\x00\x00\x00\x10x\x00\x01\x00\x00\x00\x10y\x00\x02\x00\x00\x00\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- BSONEachRow: a BSON array of arrays for a Tuple of numbers is still a mismatch"
{
    echo "CREATE TABLE t (u UUID, a Tuple(UInt8, UInt8)) ENGINE = Memory; INSERT INTO t FORMAT BSONEachRow"
    # {u: Binary(subtype UUID, 4 bytes 'AAAA'), a: [[int32 1], [int32 2]]}
    printf '\x37\x00\x00\x00\x05u\x00\x04\x00\x00\x00\x04AAAA\x04a\x00\x23\x00\x00\x00\x040\x00\x0c\x00\x00\x00\x100\x00\x01\x00\x00\x00\x00\x041\x00\x0c\x00\x00\x00\x100\x00\x02\x00\x00\x00\x00\x00\x00'
} | $CLICKHOUSE_LOCAL 2>&1 | check

echo "-- JSONEachRow: a date-shaped string is a valid value for an Enum with such a label"
echo '{"d":"2020-01-01","n":1.5}' | $CLICKHOUSE_LOCAL \
    --query "CREATE TABLE t (d Enum8('2020-01-01' = 1), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" 2>&1 | check
echo '{"d":"2020-01-01","n":1}' | $CLICKHOUSE_LOCAL \
    --query "CREATE TABLE t (d Enum8('2020-01-01' = 1), n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" 2>&1 && echo "inserted"

echo "-- JSONEachRow: a date-shaped string for a numeric column is still a mismatch"
echo '{"d":"2020-01-01","n":1.5}' | $CLICKHOUSE_LOCAL \
    --query "CREATE TABLE t (d UInt32, n UInt8) ENGINE = Memory; INSERT INTO t FORMAT JSONEachRow" 2>&1 | check
