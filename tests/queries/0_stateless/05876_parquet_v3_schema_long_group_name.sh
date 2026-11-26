#!/usr/bin/env bash

# Reading a column next to a group with a long name and many children must not cost
# O(children x name length) in the Parquet V3 schema converter.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR" "${CLICKHOUSE_USER_FILES_UNIQUE}"
trap 'rm -rf "$WORK_DIR" "${CLICKHOUSE_USER_FILES_UNIQUE:?}"' EXIT

# Hand-built Parquet files (PAR1 + FileMetaData footer, no row groups).
python3 - "$WORK_DIR" "${CLICKHOUSE_USER_FILES_UNIQUE}" <<'PYEOF'
import struct, sys
work, user_files = sys.argv[1], sys.argv[2]

class W:
    def __init__(s): s.out = bytearray()
    def u8(s, v): s.out.append(v & 0xff)
    def varint(s, v):
        v &= (1 << 64) - 1
        while True:
            b = v & 0x7f; v >>= 7
            s.out.append(b | 0x80) if v else s.out.append(b)
            if not v: return
    def zz(s, n): s.varint(((n << 1) ^ (n >> 63)) & ((1 << 64) - 1))

CT_I32, CT_I64, CT_BIN, CT_LIST, CT_STRUCT = 5, 6, 8, 9, 12
REQUIRED, OPTIONAL, REPEATED = 0, 1, 2
INT32, INT64, DOUBLE, BYTE_ARRAY = 1, 2, 5, 6

def field(w, last, fid, ctype):
    d = fid - last
    if 1 <= d <= 15: w.u8((d << 4) | ctype)
    else: w.u8(ctype); w.zz(fid)
    return fid

def schema_elem(w, name, repetition=None, num_children=None, ptype=None, file_type=False):
    last = 0
    if ptype is not None:
        last = field(w, last, 1, CT_I32); w.zz(ptype)
    if repetition is not None:
        last = field(w, last, 3, CT_I32); w.zz(repetition)
    last = field(w, last, 4, CT_BIN); w.varint(len(name)); w.out += name.encode()
    if num_children is not None:
        last = field(w, last, 5, CT_I32); w.zz(num_children)
    if file_type:
        last = field(w, last, 10, CT_STRUCT); field(w, 0, 19, CT_STRUCT); w.u8(0); w.u8(0)
    w.u8(0)

def build(path, elements):
    fmd = W(); last = 0
    last = field(fmd, last, 1, CT_I32); fmd.zz(2)                       # version
    last = field(fmd, last, 2, CT_LIST)                                  # schema
    if len(elements) < 15: fmd.u8((len(elements) << 4) | CT_STRUCT)
    else: fmd.u8((15 << 4) | CT_STRUCT); fmd.varint(len(elements))
    for e in elements:
        schema_elem(fmd, *e)
    last = field(fmd, last, 3, CT_I64); fmd.zz(0)                        # num_rows
    last = field(fmd, last, 4, CT_LIST); fmd.u8((0 << 4) | CT_STRUCT)    # row_groups = []
    fmd.u8(0)
    meta = bytes(fmd.out)
    open(path, "wb").write(b'PAR1' + meta + struct.pack('<I', len(meta)) + b'PAR1')

def long_group(path, name_size, children, repetition, child):
    elements = [("root", None, 2), ("geometry", REQUIRED, None, BYTE_ARRAY), ("a" * name_size, repetition, children)]
    elements += [child(str(i)) for i in range(children)]
    build(path, elements)

# A group named 'a' * 4000000 with 500000 DOUBLE leaves.
long_group(f"{work}/wide.parquet", 4000000, 500000, REQUIRED, lambda n: (n, REQUIRED, None, DOUBLE))
# A group named 'a' * 16000000 with 250000 `FILE` groups that have no fields.
long_group(f"{work}/file_children.parquet", 16000000, 250000, REQUIRED, lambda n: (n, REQUIRED, 0, None, True))
# A repeated group named 'a' * 1000000 with 2000 empty groups.
long_group(f"{user_files}/repeated.parquet", 1000000, 2000, REPEATED, lambda n: (n, REQUIRED, 0))
# A group `g` with one INT32 leaf with an empty name, so its path `g.` is as long as the requested name.
build(f"{work}/boundary.parquet", [("root", None, 1), ("g", REQUIRED, 1), ("", REQUIRED, None, INT32)])
# A variant group `vvvvv` as long as the requested name `value`, next to a top-level `value`.
build(f"{work}/variant.parquet", [("root", None, 2), ("vvvvv", OPTIONAL, 2),
                                  ("metadata", REQUIRED, None, BYTE_ARRAY), ("value", OPTIONAL, None, BYTE_ARRAY),
                                  ("value", REQUIRED, None, INT64)])
PYEOF

out=$(timeout 60 ${CLICKHOUSE_LOCAL} -q "SELECT geometry FROM file('${WORK_DIR}/wide.parquet', Parquet, 'geometry String') FORMAT Null SETTINGS input_format_parquet_use_native_reader_v3 = 1" 2>&1)
rc=$?
if [ "$rc" = 0 ] && [ -z "$out" ]; then echo "wide: ok"; else echo "wide: UNEXPECTED rc=$rc ${out:0:200}"; fi

out=$(timeout 60 ${CLICKHOUSE_LOCAL} -q "SELECT geometry FROM file('${WORK_DIR}/file_children.parquet', Parquet, 'geometry String') FORMAT Null SETTINGS input_format_parquet_use_native_reader_v3 = 1" 2>&1)
rc=$?
if [ "$rc" = 0 ] && [ -z "$out" ]; then echo "file: ok"; else echo "file: UNEXPECTED rc=$rc ${out:0:200}"; fi

${CLICKHOUSE_CLIENT} -q "SELECT geometry FROM file('${CLICKHOUSE_TEST_UNIQUE_NAME}/repeated.parquet', Parquet, 'geometry String') FORMAT Null SETTINGS input_format_parquet_use_native_reader_v3 = 1, log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_repeated'"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} -q "SELECT 'repeated', memory_usage < 512000000 FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_repeated' AND type = 'QueryFinish'"

out=$(${CLICKHOUSE_LOCAL} -q "SELECT * FROM file('${WORK_DIR}/boundary.parquet', Parquet, '\`g.\` Int32') SETTINGS input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_allow_missing_columns = 0" 2>&1)
if [ -z "$out" ]; then echo "boundary: ok"; else echo "boundary: UNEXPECTED ${out:0:200}"; fi

out=$(${CLICKHOUSE_LOCAL} -q "SELECT value FROM file('${WORK_DIR}/variant.parquet', Parquet, 'value Int64') SETTINGS input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_allow_missing_columns = 0, input_format_parquet_detect_variant_by_structure = 1" 2>&1)
if [ -z "$out" ]; then echo "variant: ok"; else echo "variant: UNEXPECTED ${out:0:200}"; fi

# The paths of the other columns stay intact after the children of a long-named node are read.
${CLICKHOUSE_LOCAL} -q "
    SET input_format_parquet_use_native_reader_v3 = 1;
    INSERT INTO FUNCTION file('${WORK_DIR}/nested.parquet') SELECT
        (number, number * 10, [number, number + 1], map('k', number), tuple(number * 100))::Tuple(p Int64, x Int64, arr Array(Int64), m Map(String, Int64), t Tuple(q Int64)) AS abc,
        number + 1000 AS x
    FROM numbers(3);
    SELECT x FROM file('${WORK_DIR}/nested.parquet');
    SELECT X FROM file('${WORK_DIR}/nested.parquet', Parquet, 'X Int64') SETTINGS input_format_parquet_case_insensitive_column_matching = 1;
    SELECT \`abc.t.q\`, \`abc.arr\` FROM file('${WORK_DIR}/nested.parquet', Parquet, '\`abc.t.q\` Int64, \`abc.arr\` Array(Int64)');
"
