#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: reading the XLSX format requires the minizip library.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

FILE="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.xlsx"

QUERY="
SELECT
    number AS n,
    toInt8(-number) AS i8,
    toUInt64(18446744073709551615 - number) AS u64,
    toInt128(number) * toInt128('1000000000000000000000') AS i128,
    toDecimal64(number / 3, 4) AS dec,
    number / 4 AS f64,
    toFloat32(number / 10) AS f32,
    toBool(number % 2 = 0) AS b,
    toDate('2024-01-15') + number AS d,
    toDate32('1899-12-31') + number AS d32,
    toDateTime('2024-01-15 10:30:00', 'UTC') + number * 3601 AS dt,
    toDateTime64('2024-01-15 10:30:00.123', 3, 'Asia/Tokyo') + number AS dt64,
    concat('s', toString(number), if(number = 1, ' & <x> \"q\" \n\r\t_x0041_ ', ''), if(number = 2, '\0\x01 bad \xFF utf8 ', '')) AS s,
    toFixedString('ab', 3) AS fs,
    if(number = 1, NULL, number) AS nullable,
    [number, number + 1] AS arr,
    toLowCardinality(toString(number * 10)) AS lc,
    CAST(number % 2 AS Enum8('zero' = 0, 'one' = 1)) AS e,
    toUUID('00000000-0000-0000-0000-000000000001') AS uuid,
    if(number = 0, nan, if(number = 1, -inf, 1.5)) AS special
FROM numbers(3)"

$CLICKHOUSE_LOCAL -q "$QUERY FORMAT XLSX" > "$FILE"

echo "--- inferred schema"
$CLICKHOUSE_LOCAL -q "DESCRIBE file('$FILE')"
echo "--- inferred data"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE') FORMAT TSV"
echo "--- exact round trip with the original structure"
STRUCTURE=$($CLICKHOUSE_LOCAL -q "DESCRIBE ($QUERY) FORMAT TSVRaw" | awk -F'\t' '{ printf "%s%s %s", (NR > 1 ? ", " : ""), $1, $2 }')
READ="SELECT * FROM file('$FILE', XLSX, \$\$$STRUCTURE\$\$)"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM ($READ)"
# Everything except the strings with invalid UTF-8 (n = 2) and NaN is read back exactly, in both directions.
$CLICKHOUSE_LOCAL -q "SELECT * EXCEPT (special) FROM ($READ) WHERE n != 2 EXCEPT SELECT * EXCEPT (special) FROM ($QUERY) WHERE n != 2"
$CLICKHOUSE_LOCAL -q "SELECT * EXCEPT (s, special) FROM ($QUERY) EXCEPT SELECT * EXCEPT (s, special) FROM ($READ)"
$CLICKHOUSE_LOCAL -q "SELECT n, s, special FROM ($READ) ORDER BY n FORMAT TSV"

echo "--- settings: sheet name and no header"
$CLICKHOUSE_LOCAL -q "SELECT number FROM numbers(3) FORMAT XLSX SETTINGS output_format_xlsx_sheet_name = 'Résultats', output_format_xlsx_write_header = 0" > "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE') SETTINGS input_format_excel_sheet = 'Résultats'"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$FILE', XLSX, 'x UInt64') SETTINGS input_format_excel_header = 0"

echo "--- empty result"
$CLICKHOUSE_LOCAL -q "SELECT number AS x FROM numbers(0) FORMAT XLSX" > "$FILE"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$FILE', XLSX, 'x UInt64')"

echo "--- deterministic output"
$CLICKHOUSE_LOCAL -q "SELECT number FROM numbers(1000) FORMAT XLSX" | md5sum | cut -d' ' -f1 > "$FILE.1"
$CLICKHOUSE_LOCAL -q "SELECT number FROM numbers(1000) FORMAT XLSX" | md5sum | cut -d' ' -f1 > "$FILE.2"
cmp -s "$FILE.1" "$FILE.2" && echo "same"

echo "--- insert into a table function"
$CLICKHOUSE_LOCAL -q "
    INSERT INTO FUNCTION file('$FILE', XLSX) SELECT number AS x, toString(number) AS y FROM numbers(5) SETTINGS engine_file_truncate_on_insert = 1;
    SELECT * FROM file('$FILE');"

echo "--- errors"
$CLICKHOUSE_LOCAL -q "SELECT 1 FORMAT XLSX SETTINGS output_format_xlsx_sheet_name = 'a/b'" 2>&1 | grep -o -m1 "Invalid sheet name 'a/b'"

rm -f "$FILE" "$FILE.1" "$FILE.2"
