#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: reading the XLSX format requires the minizip library.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.xlsx"

echo "--- XLS: the workbook stream of the root storage is read, not the one of an embedded object"
# nested_workbook.xls has a storage `MBD00000001` with its own `Book` stream before the `Book` stream of the root storage.
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$CURDIR/data_excel/nested_workbook.xls')"

echo "--- LowCardinality: the conversion of the dictionary type is used"
$CLICKHOUSE_LOCAL -q "SELECT 9007199254740993::UInt64 AS x, NULL::Nullable(String) AS s FORMAT XLSX" > "$FILE"
$CLICKHOUSE_LOCAL -q "
    SELECT x, s, toTypeName(x), toTypeName(s)
    FROM file('$FILE', XLSX, 'x LowCardinality(UInt64), s LowCardinality(Nullable(String))')
    SETTINGS allow_suspicious_low_cardinality_types = 1, input_format_null_as_default = 0"

echo "--- implicit time zones follow session_timezone"
$CLICKHOUSE_LOCAL -q "SELECT toDateTime('2024-01-15 12:00:00', 'UTC') AS dt, toDateTime64('2024-01-15 12:00:00.250', 3, 'UTC') AS dt64 FORMAT XLSX" > "$FILE"
$CLICKHOUSE_LOCAL -q "
    SET session_timezone = 'UTC';
    CREATE TABLE t (dt DateTime, dt64 DateTime64(3)) ENGINE = Memory;
    SET session_timezone = 'Asia/Tokyo';
    -- Excel stores the local time 12:00 without a time zone; it is interpreted in the effective time zone (Tokyo).
    INSERT INTO t SELECT * FROM file('$FILE');
    SELECT toTimeZone(dt, 'UTC'), toTimeZone(dt64, 'UTC') FROM t;
    TRUNCATE TABLE t;
    INSERT INTO t VALUES (1609459200, 1609459200.5);
    -- On output, the local time is also taken in the effective time zone.
    SELECT * FROM t INTO OUTFILE '$FILE' TRUNCATE FORMAT XLSX;
    SELECT * FROM file('$FILE', XLSX, 'dt String, dt64 String');"

echo "--- continuation sheet names are unique"
NAME='aaaaaaaaaaaaaaaaaaaaaaaaaaa (2)'
$CLICKHOUSE_LOCAL -q "SELECT 1 AS x FROM numbers(1048576) FORMAT XLSX SETTINGS output_format_xlsx_sheet_name = '$NAME'" > "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE') SETTINGS input_format_excel_sheet = '$NAME'"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE') SETTINGS input_format_excel_sheet = 'aaaaaaaaaaaaaaaaaaaaaaaaaaa (3)'"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE') SETTINGS input_format_excel_sheet = '3'" 2>&1 | grep -o -m1 "does not have a sheet '3'"

rm -f "$FILE"
