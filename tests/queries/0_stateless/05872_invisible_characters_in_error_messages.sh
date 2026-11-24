#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Invisible characters are escaped in the message of an error about a string that cannot be parsed completely
# by a conversion function to a type where such characters are never valid, such as a number or a date.
# The rest of the message, such as the query in `In scope` or `while executing`, is kept as is, so it is cut off.

function error_message()
{
    $CLICKHOUSE_LOCAL -q "$1" 2>&1 | LC_ALL=C sed -n 's/.*DB::Exception: //p' | LC_ALL=C sed -E 's/: (In scope|while executing|while converting) .*//' | head -n1
}

echo "nothing to escape"
error_message "SELECT toUInt32('765870abc')"
echo "visible characters are not escaped"
error_message "SELECT toUInt32('765870é')"
error_message "SELECT toUInt32('1你😀')"

echo "byte order mark"
error_message "SELECT toUInt32('\xEF\xBB\xBF765870')"
echo "zero width space"
error_message "SELECT toUInt32('765\xE2\x80\x8B870')"
echo "right-to-left override"
error_message "SELECT toUInt32('1\xE2\x80\xAE23')"
echo "control characters"
error_message "SELECT toUInt32('765870\x01')"
error_message "SELECT toUInt32('1\x7F')"
echo "combining character"
error_message "SELECT toUInt32('7\xCD\x8F65870')"
echo "unassigned, private use and noncharacter code points"
error_message "SELECT toUInt32('1\xCD\xB8')"
error_message "SELECT toUInt32('1\xEE\x80\x80')"
error_message "SELECT toUInt32('1\xEF\xBF\xBE')"
echo "invalid UTF-8"
error_message "SELECT toUInt32('765870\xFF')"
error_message "SELECT toUInt32('1\x80')"
echo "several characters and a truncated sequence"
error_message "SELECT toUInt32('1\x012\xE2\x80')"

echo "the result is a string literal"
error_message "SELECT toUInt32('1\'\\\\\n\t\0\x01')"
echo "text that looks like an escape sequence differs from an escaped byte"
error_message "SELECT toUInt32('1\\\\xEF\xEF')"

echo "other types"
error_message "SELECT toFloat64('1.5\xE2\x80\x8B')"
error_message "SELECT toDecimal32('1.5\xE2\x80\x8B', 1)"
error_message "SELECT toDate('2020-01-01\xE2\x80\x8B')"
error_message "SELECT toDateTime('2020-01-01 00:00:00\xE2\x80\x8B')"
error_message "SELECT toUUID('00000000-0000-0000-0000-000000000000\xE2\x80\x8B')"
error_message "SELECT toIPv4('127.0.0.1\xE2\x80\x8B')"
error_message "SELECT toIPv6('::1\xE2\x80\x8B')"
echo "other sources"
error_message "SELECT CAST('1\xE2\x80\x8B' AS UInt32)"
error_message "SELECT toUInt32(toFixedString('1\xE2\x80\x8B', 4))"
error_message "SELECT toUInt32(materialize('1\xE2\x80\x8B'))"

echo "values from a table"
# The tables are temporary, so that nothing is left in the `default` database of clickhouse-local.
table="CREATE TEMPORARY TABLE t (s String) ENGINE = MergeTree ORDER BY tuple(); INSERT INTO t VALUES ('42'), ('\xEF\xBB\xBF765870');"
error_message "$table SELECT toUInt32(s) FROM t"
error_message "$table CREATE TEMPORARY TABLE n (x UInt32) ENGINE = Memory; INSERT INTO n SELECT s FROM t"

echo "functions that do not throw are not affected"
$CLICKHOUSE_LOCAL -q "SELECT toUInt32OrNull('\xEF\xBB\xBF765870'), toUInt32OrZero('\xEF\xBB\xBF765870')"
