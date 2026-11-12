#!/usr/bin/env bash
# Tags: no-fasttest, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Compatibility ledger regression test for the DWARF input format:
# - vendor and special forms: DW_FORM_LLVM_addrx_offset, DW_FORM_data16, DW_FORM_indirect,
#   DW_FORM_implicit_const, DW_FORM_ref_sig8/ref_sup4/ref_sup8/GNU_ref_alt, DW_FORM_strp_sup;
# - root unit DW_AT_str_offsets_base resolved even when DW_FORM_strx1 precedes it;
# - root DW_AT_GNU_addr_base used for attr_int but ignored by the ranges pre-pass;
# - DWARF4 .debug_ranges base selection, tombstone, and wrapping;
# - DWARF5 .debug_rnglists rnglistx indirection and entry kinds;
# - DWARF4 1-based vs DWARF5 0-based filename tables and <error> recovery;
# - piped input and dependency-pair column projections;
# - rejection of spec-invalid DWARF encodings with CANNOT_PARSE_DWARF.

WORK_DIR="${CLICKHOUSE_TMP}/dwarf_ledger_$$"
mkdir -p "$WORK_DIR"

python3 - "$WORK_DIR" <<'PY'
import struct, sys, os
work = sys.argv[1]

SHT_PROGBITS, SHT_STRTAB = 1, 3

def uleb(v):
    out = bytearray()
    while True:
        b = v & 0x7f
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)

def sleb(v):
    out = bytearray()
    more = True
    while more:
        b = v & 0x7f
        v >>= 7
        if (v == 0 and (b & 0x40) == 0) or (v == -1 and (b & 0x40) != 0):
            more = False
        else:
            b |= 0x80
        out.append(b)
    return bytes(out)

def make_elf(sections_dict):
    shstr = bytearray(b'\x00.shstrtab\x00')
    sec_entries = []
    for name, data in sections_dict.items():
        name_off = len(shstr)
        shstr.extend(name.encode('ascii') + b'\x00')
        sec_entries.append((name_off, data))

    data_off = 64 + 56
    cur = data_off
    shstr_off = cur
    cur += len(shstr)
    placed = []
    for name_off, data in sec_entries:
        placed.append((name_off, cur, len(data), data))
        cur += len(data)
    pad = (8 - (cur % 8)) % 8
    cur += pad
    shoff = cur

    def sh(name, stype, off, size):
        return struct.pack('<IIQQQQIIQQ', name, stype, 0, 0, off, size, 0, 0, 1, 0)

    shdrs = [
        sh(0, 0, 0, 0),
        sh(1, SHT_STRTAB, shstr_off, len(shstr)),
    ]
    for name_off, off, size, _ in placed:
        shdrs.append(sh(name_off, SHT_PROGBITS, off, size))

    ident = b'\x7fELF' + bytes([2, 1, 1]) + bytes(9)
    ehdr = ident + struct.pack('<HHIQQQIHHHHHH', 2, 0x3e, 1, 0, 64, shoff, 0,
                               64, 56, 1, 64, len(shdrs), 1)
    phdr = struct.pack('<IIQQQQQQ', 1, 4, 0, 0, 0, 0, 0, 8)
    body = bytearray(ehdr + phdr + shstr)
    for _, _, _, data in placed:
        body.extend(data)
    body.extend(b'\x00' * pad)
    for s in shdrs:
        body.extend(s)
    return bytes(body)

# 1. DWARF5 valid fixture: strx1 BEFORE str_offsets_base, LLVM_addrx_offset, data16,
#    ref_sig8/ref_sup4/ref_sup8/GNU_ref_alt, strp_sup, indirect, implicit_const, rnglistx, line v5.
debug_str = b'cu_from_strx\x00sup_str\x00'
# .debug_str_offsets: 32-bit header (length=8, version=5, pad=0) + 1 entry at offset 8 -> 0
debug_str_offsets = struct.pack('<IHHI', 8, 5, 0, 0)
# .debug_addr: 2 entries at offset 0: 0x1000, 0x2000
debug_addr = struct.pack('<QQ', 0x1000, 0x2000)
# .debug_rnglists: offset 0 table [4], at offset 4: DW_RLE_startx_length(idx=1, len=0x30), DW_RLE_end_of_list(0)
debug_rnglists = struct.pack('<I', 4) + bytes([0x03, 0x01, 0x30, 0x00])
# .debug_line v5 header with 2 files: file 0 = inline "f0.c", file 1 = line_strp OOB -> "<error>"
line_body = bytearray()
line_body += struct.pack('<HBB', 5, 8, 0)
prose = bytearray()
prose += bytes([1, 1, 1, 0xfb, 14, 13]) + bytes([0] * 12)
prose += bytes([1, 0x01, 0x08, 1]) + b'dir\x00' # 1 dir
prose += bytes([1, 0x01, 0x1f, 2]) + struct.pack('<II', 0, 9999) # 2 files via DW_FORM_line_strp (0x1f)
line_body += struct.pack('<I', len(prose)) + prose
debug_line_v5 = struct.pack('<I', len(line_body)) + line_body
debug_line_str = b'f0.c\x00'

# Abbrev:
# 1: DW_TAG_compile_unit (0x11), children=yes
#    DW_AT_name (0x03) / DW_FORM_strx1 (0x25)  <-- BEFORE DW_AT_str_offsets_base!
#    DW_AT_str_offsets_base (0x72) / DW_FORM_sec_offset (0x17)
#    DW_AT_addr_base (0x73) / DW_FORM_sec_offset (0x17)
#    DW_AT_rnglists_base (0x74) / DW_FORM_sec_offset (0x17)
#    DW_AT_stmt_list (0x10) / DW_FORM_sec_offset (0x17)
# 2: DW_TAG_subprogram (0x2e), children=no
#    DW_AT_entry_pc (0x52) / DW_FORM_LLVM_addrx_offset (0x2001)
#    DW_AT_const_value (0x1c) / DW_FORM_data16 (0x1e)
#    DW_AT_type (0x49) / DW_FORM_ref_sig8 (0x20)
#    DW_AT_specification (0x47) / DW_FORM_ref_sup4 (0x1c)
#    DW_AT_abstract_origin (0x31) / DW_FORM_ref_sup8 (0x24)
#    DW_AT_import (0x18) / DW_FORM_GNU_ref_alt (0x1f20)
#    DW_AT_description (0x5a) / DW_FORM_strp_sup (0x1d)
#    DW_AT_decl_line (0x3b) / DW_FORM_indirect (0x16) -> DW_FORM_udata (0x0f)
#    DW_AT_decl_column (0x39) / DW_FORM_implicit_const (0x21) = 42
#    DW_AT_decl_file (0x3a) / DW_FORM_data1 (0x0b)
#    DW_AT_call_file (0x58) / DW_FORM_data1 (0x0b)
#    DW_AT_ranges (0x55) / DW_FORM_rnglistx (0x23)
abbrev_v5 = (
    uleb(1) + uleb(0x11) + bytes([1]) +
    uleb(0x03) + uleb(0x25) +
    uleb(0x72) + uleb(0x17) +
    uleb(0x73) + uleb(0x17) +
    uleb(0x74) + uleb(0x17) +
    uleb(0x10) + uleb(0x17) +
    bytes([0, 0]) +
    uleb(2) + uleb(0x2e) + bytes([0]) +
    uleb(0x52) + uleb(0x2001) +
    uleb(0x1c) + uleb(0x1e) +
    uleb(0x49) + uleb(0x20) +
    uleb(0x47) + uleb(0x1c) +
    uleb(0x31) + uleb(0x24) +
    uleb(0x18) + uleb(0x1f20) +
    uleb(0x5a) + uleb(0x1d) +
    uleb(0x3b) + uleb(0x16) +
    uleb(0x39) + uleb(0x21) + sleb(42) +
    uleb(0x3a) + uleb(0x0b) +
    uleb(0x58) + uleb(0x0b) +
    uleb(0x55) + uleb(0x23) +
    bytes([0, 0, 0])
)

die1 = bytes([1, 0]) + struct.pack('<IIII', 8, 0, 0, 0)
die2 = (
    bytes([2]) +
    uleb(1) + struct.pack('<I', 0x55) + # LLVM_addrx_offset: idx=1 (0x2000) + 0x55 = 0x2055
    bytes(range(16)) + # data16
    struct.pack('<Q', 0xdeadbeef) + # ref_sig8
    struct.pack('<I', 0x1234) + # ref_sup4
    struct.pack('<Q', 0x5678) + # ref_sup8
    struct.pack('<I', 0x9abc) + # GNU_ref_alt
    struct.pack('<I', 13) + # strp_sup -> "sup_str"
    uleb(0x0f) + uleb(77) + # indirect -> udata(77)
    bytes([0]) + # decl_file = 0 ("f0.c")
    bytes([1]) + # call_file = 1 ("<error>")
    uleb(0) # rnglistx = 0 -> [0x2000, 0x2030)
)
dies_v5 = die1 + die2 + bytes([0])
info_v5_body = struct.pack('<HBBI', 5, 1, 8, 0) + dies_v5
info_v5 = struct.pack('<I', len(info_v5_body)) + info_v5_body

open(os.path.join(work, 'valid_v5.elf'), 'wb').write(make_elf({
    '.debug_abbrev': abbrev_v5,
    '.debug_info': info_v5,
    '.debug_str': debug_str,
    '.debug_str_offsets': debug_str_offsets,
    '.debug_addr': debug_addr,
    '.debug_rnglists': debug_rnglists,
    '.debug_line': debug_line_v5,
    '.debug_line_str': debug_line_str,
}))

# 1b. DWARF4 valid fixture: root DW_AT_GNU_addr_base=8 used by attr_int (low_pc -> 0x3000,
#     entry_pc via DW_FORM_GNU_addr_index(1) -> 0x4000) while prepass ignores DW_AT_GNU_addr_base
#     (unit base_address reads .debug_addr[0] = 0x1000), DWARF4 .debug_ranges base selection,
#     tombstone and wrapping, and DWARF4 1-based .debug_line filename table.
debug_str_v4 = b'cu_v4\x00'
debug_addr_v4 = struct.pack('<QQQ', 0x1000, 0x3000, 0x4000)
debug_ranges_v4 = (
    struct.pack('<QQ', 0x10, 0x20) +
    struct.pack('<QQ', 0xffffffffffffffff, 0x5000) +
    struct.pack('<QQ', 0x1, 0x3) +
    struct.pack('<QQ', 0xfffffffffffffffe, 0x9) +
    struct.pack('<QQ', 0xfffffffffffffffd, 0x9) +
    struct.pack('<QQ', 0, 0)
)
prose_v4 = (
    bytes([1, 1, 1, 0xfb, 14, 13]) +
    bytes([0] * 12) +
    b'dir\x00\x00' +
    b'v4.c\x00\x01\x00\x00\x00'
)
line_body_v4 = struct.pack('<HI', 4, len(prose_v4)) + prose_v4
debug_line_v4 = struct.pack('<I', len(line_body_v4)) + line_body_v4
abbrev_v4 = (
    uleb(1) + uleb(0x11) + bytes([1]) +
    uleb(0x03) + uleb(0x0e) +
    uleb(0x2133) + uleb(0x17) +
    uleb(0x11) + uleb(0x01) +
    uleb(0x10) + uleb(0x17) +
    bytes([0, 0]) +
    uleb(2) + uleb(0x2e) + bytes([0]) +
    uleb(0x52) + uleb(0x1f01) +
    uleb(0x3a) + uleb(0x0b) +
    uleb(0x58) + uleb(0x0b) +
    uleb(0x55) + uleb(0x17) +
    bytes([0, 0, 0])
)
die1_v4 = bytes([1]) + struct.pack('<IIQI', 0, 8, 0x1000, 0)
die2_v4 = bytes([2]) + uleb(1) + bytes([1, 0]) + struct.pack('<I', 0)
dies_v4 = die1_v4 + die2_v4 + bytes([0])
info_v4_body = struct.pack('<HIB', 4, 0, 8) + dies_v4
info_v4 = struct.pack('<I', len(info_v4_body)) + info_v4_body
open(os.path.join(work, 'valid_v4.elf'), 'wb').write(make_elf({
    '.debug_abbrev': abbrev_v4,
    '.debug_info': info_v4,
    '.debug_str': debug_str_v4,
    '.debug_addr': debug_addr_v4,
    '.debug_ranges': debug_ranges_v4,
    '.debug_line': debug_line_v4,
}))

# 1c. DWARF5 line table with 4096 DW_FORM_line_strp entries all pointing to one 1 MiB string in
#     .debug_line_str: SELECT offset stays well within a 32 MiB memory limit (zero-copy filename
#     table), while materializing 64 copies of that 1 MiB decl_file hits MEMORY_LIMIT_EXCEEDED.
prose_rep = bytearray([1, 1, 1, 0xfb, 14, 13] + [0] * 12 + [1, 0x01, 0x08, 1]) + b'dir\x00' + bytes([1, 0x01, 0x1f]) + uleb(4096) + (struct.pack('<I', 0) * 4096)
line_body_rep = struct.pack('<HBBI', 5, 8, 0, len(prose_rep)) + prose_rep
debug_line_rep = struct.pack('<I', len(line_body_rep)) + line_body_rep
big_line_str = (b'a' * (1024 * 1024)) + b'\x00'
abbrev_rep = (
    uleb(1) + uleb(0x11) + bytes([1]) + uleb(0x10) + uleb(0x17) + bytes([0, 0]) +
    uleb(2) + uleb(0x2e) + bytes([0]) + uleb(0x3a) + uleb(0x0f) + bytes([0, 0, 0])
)
dies_rep = bytes([1]) + struct.pack('<I', 0) + (bytes([2]) + uleb(4095)) * 64 + bytes([0])
info_rep_body = struct.pack('<HBBI', 5, 1, 8, 0) + dies_rep
info_rep = struct.pack('<I', len(info_rep_body)) + info_rep_body
open(os.path.join(work, 'repeated_line_strp.elf'), 'wb').write(make_elf({
    '.debug_abbrev': abbrev_rep,
    '.debug_info': info_rep,
    '.debug_line': debug_line_rep,
    '.debug_line_str': big_line_str,
}))

# 2. Malformed DWARF cases that must fail with CANNOT_PARSE_DWARF:
# a) addr_size = 1
bad_addr1_body = struct.pack('<IHIB', 2 + 4 + 1 + 2, 4, 0, 1) + bytes([1, 0])
open(os.path.join(work, 'bad_addr_size_1.elf'), 'wb').write(make_elf({
    '.debug_abbrev': bytes([1, 0x11, 0, 0, 0, 0]),
    '.debug_info': bad_addr1_body,
}))

# b) duplicate abbreviation code
dup_abbrev = bytes([1, 0x11, 0, 0, 0, 1, 0x11, 0, 0, 0, 0])
valid_u4 = struct.pack('<IHIB', 2 + 4 + 1 + 1, 4, 0, 8) + bytes([1])
open(os.path.join(work, 'dup_abbrev.elf'), 'wb').write(make_elf({
    '.debug_abbrev': dup_abbrev,
    '.debug_info': valid_u4,
}))

# c) children byte = 2
bad_child = bytes([1, 0x11, 2, 0, 0, 0])
open(os.path.join(work, 'bad_children_byte.elf'), 'wb').write(make_elf({
    '.debug_abbrev': bad_child,
    '.debug_info': valid_u4,
}))

# d) unknown form 0x3fff
unk_form = uleb(1) + uleb(0x11) + bytes([0]) + uleb(0x03) + uleb(0x3fff) + bytes([0, 0, 0])
open(os.path.join(work, 'unknown_form.elf'), 'wb').write(make_elf({
    '.debug_abbrev': unk_form,
    '.debug_info': valid_u4,
}))

# e) second unit header truncated after valid first unit
open(os.path.join(work, 'truncated_second_unit.elf'), 'wb').write(make_elf({
    '.debug_abbrev': bytes([1, 0x11, 0, 0, 0, 0]),
    '.debug_info': valid_u4 + b'\x10\x00\x00',
}))
PY

${CLICKHOUSE_LOCAL} -q "
SELECT offset, size, tag, unit_name, unit_offset, ancestor_tags, name, decl_file, decl_line, ranges, attr_name, attr_form, attr_int, attr_str
FROM file('${WORK_DIR}/valid_v5.elf', DWARF)
ORDER BY offset
"

${CLICKHOUSE_LOCAL} -q "
SELECT offset, size, tag, unit_name, unit_offset, ancestor_tags, name, decl_file, decl_line, ranges, attr_name, attr_form, attr_int, attr_str
FROM file('${WORK_DIR}/valid_v4.elf', DWARF)
ORDER BY offset
"

# Dependency-pair projections and piped input
${CLICKHOUSE_LOCAL} -q "SELECT ancestor_offsets FROM file('${WORK_DIR}/valid_v5.elf', DWARF) ORDER BY ancestor_offsets"
${CLICKHOUSE_LOCAL} -q "SELECT attr_int FROM file('${WORK_DIR}/valid_v5.elf', DWARF) WHERE length(attr_int) > 0"
cat "${WORK_DIR}/valid_v5.elf" | ${CLICKHOUSE_LOCAL} --input-format DWARF -q "SELECT count(), sum(cityHash64(*)) FROM table"

${CLICKHOUSE_LOCAL} -q "SELECT count() FROM file('${WORK_DIR}/repeated_line_strp.elf', DWARF) SETTINGS max_memory_usage = 33554432"
if ${CLICKHOUSE_LOCAL} -q "SELECT sum(length(decl_file)) FROM file('${WORK_DIR}/repeated_line_strp.elf', DWARF) SETTINGS max_memory_usage = 33554432" >/dev/null 2>"${WORK_DIR}/err_mem"
then
    echo "repeated_line_strp: UNEXPECTED SUCCESS"
elif grep -q "MEMORY_LIMIT_EXCEEDED" "${WORK_DIR}/err_mem"
then
    echo "repeated_line_strp: memory limit enforced"
else
    echo "repeated_line_strp: UNEXPECTED ERROR"
    cat "${WORK_DIR}/err_mem"
fi

for bad in bad_addr_size_1 dup_abbrev bad_children_byte unknown_form truncated_second_unit
do
    if ${CLICKHOUSE_LOCAL} -q "SELECT count() FROM file('${WORK_DIR}/${bad}.elf', DWARF)" >/dev/null 2>"${WORK_DIR}/err"
    then
        echo "${bad}: UNEXPECTED SUCCESS"
    elif grep -q "CANNOT_PARSE_DWARF" "${WORK_DIR}/err"
    then
        echo "${bad}: rejected DWARF"
    else
        echo "${bad}: UNEXPECTED ERROR"
        cat "${WORK_DIR}/err"
    fi
done

rm -rf "$WORK_DIR"
