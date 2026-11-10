#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the Parquet format is not available in the fast test build.

# `Parquet` `VARIANT` encodings that are valid per the specification but that the ClickHouse writer
# does not produce by itself. The files are written by ClickHouse (without page checksums) and the
# residual `value` bytes are patched in place:
#   - a `DECIMAL4` with scale 10 and a `DECIMAL8` with scale 20: the scale does not fit the
#     carrier of the physical width, so a wider decimal type must be used;
#   - an object whose field offsets follow field-name order while the values are stored in the
#     opposite physical order, so the offsets neither start at zero nor increase.
# Also checks that a residual `String` written with `output_format_parquet_string_as_string = 0`
# uses the `binary` primitive and is read back with its raw bytes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR"
trap 'rm -rf "$WORK_DIR"' EXIT

# A single non-majority row stays in the residual `value` column, next to two `String` rows.
${CLICKHOUSE_LOCAL} --query "
SET output_format_parquet_compression_method = 'none', output_format_parquet_use_custom_encoder = 1,
    output_format_parquet_write_checksums = 0, enable_json_type = 1;
SELECT v FROM (SELECT 1 AS k, CAST(toDecimal32(1.5, 1), 'Dynamic') AS v UNION ALL SELECT 2, CAST('x', 'Dynamic') UNION ALL SELECT 3, CAST('y', 'Dynamic'))
ORDER BY k INTO OUTFILE '${WORK_DIR}/d4.parquet' FORMAT Parquet;
SELECT v FROM (SELECT 1 AS k, CAST(toDecimal64(1.5, 1), 'Dynamic') AS v UNION ALL SELECT 2, CAST('x', 'Dynamic') UNION ALL SELECT 3, CAST('y', 'Dynamic'))
ORDER BY k INTO OUTFILE '${WORK_DIR}/d8.parquet' FORMAT Parquet;
SELECT v FROM (SELECT 1 AS k, CAST('{\"a\":1,\"b\":2}'::JSON, 'Dynamic') AS v UNION ALL SELECT 2, CAST('x', 'Dynamic') UNION ALL SELECT 3, CAST('y', 'Dynamic'))
ORDER BY k INTO OUTFILE '${WORK_DIR}/o.parquet' FORMAT Parquet;
SELECT v FROM (SELECT 1 AS k, CAST(unhex('00FF01FE')::String, 'Dynamic') AS v UNION ALL SELECT 2, CAST(toInt64(1), 'Dynamic') UNION ALL SELECT 3, CAST(toInt64(2), 'Dynamic'))
ORDER BY k INTO OUTFILE '${WORK_DIR}/b.parquet' FORMAT Parquet SETTINGS output_format_parquet_string_as_string = 0;
SELECT j FROM (SELECT 1 AS k, '{\"a\":1,\"b\":2}'::JSON(max_dynamic_paths = 0) AS j UNION ALL SELECT 2, '{\"a\":\"s\"}'::JSON(max_dynamic_paths = 0) UNION ALL SELECT 3, '{\"c\":[1, \"x\"]}'::JSON(max_dynamic_paths = 0))
ORDER BY k INTO OUTFILE '${WORK_DIR}/j.parquet' FORMAT Parquet SETTINGS output_format_parquet_json_as_variant = 1;
" </dev/null

python3 - "$WORK_DIR" <<'PY'
import sys
work = sys.argv[1]

def patch(name, old, new, out):
    data = bytearray(open(f"{work}/{name}", "rb").read())
    old = bytes.fromhex(old)
    new = bytes.fromhex(new)
    assert len(old) == len(new)
    pos = data.find(old)
    if pos < 0:
        raise RuntimeError(f"pattern not found in {name}")
    data[pos:pos + len(old)] = new
    open(f"{work}/{out}", "wb").write(data)

# DECIMAL4 (header 0x20), scale 1 -> 10, unscaled 15.
patch("d4.parquet", "20010f000000", "200a0f000000", "d4_scale10.parquet")
# DECIMAL8 (header 0x24), scale 1 -> 20, unscaled 15.
patch("d8.parquet", "24010f00000000000000", "24140f00000000000000", "d8_scale20.parquet")
# Object {"a":1,"b":2}: field ids [0,1], offsets [0,9,18] -> [9,0,18], INT64 values swapped.
patch("o.parquet",
      "02020001000912" + "180100000000000000" + "180200000000000000",
      "02020001090012" + "180200000000000000" + "180100000000000000",
      "o_reordered.parquet")
patch("j.parquet",
      "02020001000912" + "180100000000000000" + "180200000000000000",
      "02020001090012" + "180200000000000000" + "180100000000000000",
      "j_reordered.parquet")

data = open(f"{work}/b.parquet", "rb").read()
# `binary` primitive (header 0x3c), length 4, raw bytes.
print("binary primitive written:", data.find(bytes.fromhex("3c0400000000ff01fe")) >= 0)
PY

echo "-- decimal scale wider than the physical carrier"
${CLICKHOUSE_LOCAL} --query "SELECT v, dynamicType(v) FROM file('${WORK_DIR}/d4_scale10.parquet', Parquet) LIMIT 1"
${CLICKHOUSE_LOCAL} --query "SELECT v, dynamicType(v) FROM file('${WORK_DIR}/d8_scale20.parquet', Parquet) LIMIT 1"
${CLICKHOUSE_LOCAL} --query "SELECT v.\`Decimal(18, 10)\` FROM file('${WORK_DIR}/d4_scale10.parquet', Parquet, 'v Dynamic') LIMIT 1"

echo "-- object offsets in field-name order"
${CLICKHOUSE_LOCAL} --query "SELECT v, dynamicType(v) FROM file('${WORK_DIR}/o_reordered.parquet', Parquet) LIMIT 1"
${CLICKHOUSE_LOCAL} --query "SELECT v.JSON.a, v.JSON.b FROM file('${WORK_DIR}/o_reordered.parquet', Parquet, 'v Dynamic') LIMIT 1"
${CLICKHOUSE_LOCAL} --query "SELECT v FROM file('${WORK_DIR}/o_reordered.parquet', Parquet, 'v String') LIMIT 1"

echo "-- projected reads of an unshredded JSON column; a row without the path reads as missing"
for f in j j_reordered; do
    ${CLICKHOUSE_LOCAL} --query "SELECT j FROM file('${WORK_DIR}/${f}.parquet', Parquet, 'j JSON')"
    ${CLICKHOUSE_LOCAL} --query "SELECT j.a, j.b FROM file('${WORK_DIR}/${f}.parquet', Parquet, 'j JSON(b Int64)')"
    ${CLICKHOUSE_LOCAL} --query "SELECT j.a FROM file('${WORK_DIR}/${f}.parquet', Parquet, 'j JSON(a Dynamic)')"
    ${CLICKHOUSE_LOCAL} --query "SELECT \`j.b\`, \`j.a\` FROM file('${WORK_DIR}/${f}.parquet', Parquet, '\`j.b\` Nullable(Int64), \`j.a\` Dynamic')"
done

echo "-- binary residual keeps its raw bytes"
${CLICKHOUSE_LOCAL} --query "SELECT dynamicType(v), hex(v::String) FROM file('${WORK_DIR}/b.parquet', Parquet) LIMIT 1"
${CLICKHOUSE_LOCAL} --query "SELECT hex(v.String) FROM file('${WORK_DIR}/b.parquet', Parquet, 'v Dynamic') LIMIT 1"
