#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the XLSX format requires the minizip library.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

$CLICKHOUSE_LOCAL -q "SELECT 'abc' AS s FORMAT XLSX" > "$WORK_DIR/base.xlsx"
# Many rows with characters outside of the Basic Multilingual Plane, so that the UTF-16 part spans many chunks of the conversion.
$CLICKHOUSE_LOCAL -q "SELECT concat('row ', toString(number), ' 😀é') AS s FROM numbers(100000) FORMAT XLSX" > "$WORK_DIR/many.xlsx"

python3 - "$WORK_DIR" <<'PYTHON'
import sys
import zipfile

work_dir = sys.argv[1]

def rewrite(source, target, transform):
    with zipfile.ZipFile(source) as zin, zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as zout:
        for item in zin.infolist():
            zout.writestr(item.filename, transform(item.filename, zin.read(item.filename)))

def replace_cell(target, replacement):
    rewrite(f"{work_dir}/base.xlsx", f"{work_dir}/{target}.xlsx",
        lambda filename, content: content.replace(b'<c r="A2" t="inlineStr"><is><t>abc</t></is></c>', replacement)
            if filename == "xl/worksheets/sheet1.xml" else content)

# A literal text split between rich text runs must not form an escape sequence `_xHHHH_`, while an escape sequence in one run is decoded.
replace_cell("split_escape", b'<c r="A2" t="inlineStr"><is><r><t>_x00</t></r><r><t>41_</t></r><r><t>|_x0042_</t></r></is></c>')

# Impossible calendar dates in cells of the date type are rejected.
for name, value in [("feb_31", "2024-02-31"), ("feb_29_non_leap", "2023-02-29"), ("feb_29_1900", "1900-02-29"), ("apr_31", "2024-04-31"),
                    ("feb_29_leap", "2024-02-29"), ("feb_29_2000", "2000-02-29T10:00:00")]:
    replace_cell(name, f'<c r="A2" t="d"><v>{value}</v></c>'.encode())

# The worksheet is encoded in UTF-16.
for name, encoding, bom in [("many_utf16le", "utf-16-le", b"\xff\xfe"), ("many_utf16be", "utf-16-be", b"")]:
    rewrite(f"{work_dir}/many.xlsx", f"{work_dir}/{name}.xlsx",
        lambda filename, content: bom + content.decode("utf-8").encode(encoding) if filename == "xl/worksheets/sheet1.xml" else content)
PYTHON

echo "--- escapes split between runs"
$CLICKHOUSE_LOCAL -q "SELECT * FROM file('$WORK_DIR/split_escape.xlsx', XLSX)"

echo "--- impossible dates"
for name in feb_31 feb_29_non_leap feb_29_1900 apr_31; do
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$WORK_DIR/$name.xlsx', XLSX)" 2>&1 | grep -o -m1 "Cannot parse the date '[0-9-]*'"
done
for name in feb_29_leap feb_29_2000; do
    $CLICKHOUSE_LOCAL -q "SELECT * FROM file('$WORK_DIR/$name.xlsx', XLSX)"
done

echo "--- UTF-16 worksheet"
for name in many many_utf16le many_utf16be; do
    $CLICKHOUSE_LOCAL -q "SELECT count(), sum(s = concat('row ', toString(rowNumberInAllBlocks()), ' 😀é')) FROM file('$WORK_DIR/$name.xlsx', XLSX)"
done
