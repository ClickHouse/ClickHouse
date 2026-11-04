#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the XLSX format requires the minizip library.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_DIR="$CUR_DIR/data_excel"
WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

$CLICKHOUSE_LOCAL -q "SELECT 'abc' AS s, 1 AS n FORMAT XLSX" > "$WORK_DIR/base.xlsx"
for value in "2262-04-11 23:47:16.854" "2262-04-11 23:47:16.855"; do
    $CLICKHOUSE_LOCAL -q "SELECT toDateTime64('$value', 3, 'UTC') AS a FORMAT XLSX" > "$WORK_DIR/datetime64_${value: -3}.xlsx"
done
$CLICKHOUSE_LOCAL -q "SELECT toDate32('2263-01-01') AS d FORMAT XLSX" > "$WORK_DIR/date_2263.xlsx"

python3 - "$DATA_DIR" "$WORK_DIR" <<'PYTHON'
import struct
import sys
import zipfile

data_dir, work_dir = sys.argv[1], sys.argv[2]

def rewrite(source, target, transform):
    with zipfile.ZipFile(source) as zin, zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            zout.writestr(item.filename, transform(item.filename, zin.read(item.filename)))

# The parts of an Office Open XML package can be encoded in UTF-16, with or without a byte order mark, or have a UTF-8 byte order mark.
for name, encoding, bom in [("utf16le_bom", "utf-16-le", b"\xff\xfe"), ("utf16be", "utf-16-be", b""), ("utf8_bom", "utf-8", b"\xef\xbb\xbf")]:
    def transform(filename, content):
        if filename in ("_rels/.rels", "xl/workbook.xml", "xl/_rels/workbook.xml.rels", "xl/worksheets/sheet1.xml"):
            return bom + content.decode("utf-8").encode(encoding)
        return content
    rewrite(f"{data_dir}/sample.xlsx", f"{work_dir}/{name}.xlsx", transform)

# Deeply nested rich text runs must not exhaust the stack.
depth = 100000
rewrite(f"{work_dir}/base.xlsx", f"{work_dir}/nested_runs.xlsx",
    lambda filename, content: content.replace(b"<is><t>abc</t></is>", b"<is>" + b"<r>" * depth + b"<t>deep</t>" + b"</r>" * depth + b"</is>")
        if filename == "xl/worksheets/sheet1.xml" else content)

xls = open(f"{data_dir}/sample.xls", "rb").read()

# A huge number of DIFAT sectors.
corrupted = bytearray(xls)
struct.pack_into("<II", corrupted, 68, 1, 0xFFFFFFF0)
open(f"{work_dir}/difat_count.xls", "wb").write(corrupted)

# A cyclic chain of DIFAT sectors: the first DIFAT sector links to itself.
corrupted = bytearray(xls)
struct.pack_into("<II", corrupted, 68, 0, 3)
for i in range(127):
    struct.pack_into("<I", corrupted, 512 + i * 4, 0xFFFFFFFF)
struct.pack_into("<I", corrupted, 512 + 127 * 4, 0)
open(f"{work_dir}/difat_cycle.xls", "wb").write(corrupted)
PYTHON

echo "--- UTF-16 and UTF-8 with BOM parts"
$CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$DATA_DIR/sample.xlsx', XLSX)"
for name in utf16le_bom utf16be utf8_bom; do
    $CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$WORK_DIR/$name.xlsx', XLSX)"
done

echo "--- nested rich text runs"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$WORK_DIR/nested_runs.xlsx', XLSX)"

echo "--- malformed DIFAT"
for name in difat_count difat_cycle; do
    $CLICKHOUSE_LOCAL -q "SELECT count() FROM file('$WORK_DIR/$name.xls', XLS)" 2>&1 | grep -o -m1 "The XLS file is corrupted: [a-zA-Z ]*"
done

echo "--- DateTime64 range"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$WORK_DIR/datetime64_854.xlsx', XLSX, 'a DateTime64(9, \\'UTC\\')')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$WORK_DIR/datetime64_855.xlsx', XLSX, 'a DateTime64(9, \\'UTC\\')')" 2>&1 | grep -o -m1 "is out of the range of the \`DateTime64(9)\` type"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$WORK_DIR/datetime64_855.xlsx', XLSX, 'a DateTime64(3, \\'UTC\\')')"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$WORK_DIR/date_2263.xlsx', XLSX, 'd DateTime64(9, \\'UTC\\')')" 2>&1 | grep -o -m1 "is out of the range of the \`DateTime64(9)\` type"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$WORK_DIR/date_2263.xlsx', XLSX, 'd DateTime64(6, \\'UTC\\')')"
