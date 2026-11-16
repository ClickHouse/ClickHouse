#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: the `DWARF` format is not built in the fast test.
# Tag no-msan: the `DWARF` format is not built with MSan.

# An attribute whose length goes past the end of `.debug_info` must be rejected by the `DWARF` format.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

prefix="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"

# A minimal ELF with one compilation unit with two attributes of form DW_FORM_exprloc either in the unit entry
# or in its child. The length of the first expression is either 1 (valid), 0xFFFFFFFF (past the end of the section)
# or 0xFFFFFFFFFFFFFFFF (makes the offset wrap around to before the attribute).
python3 - "$prefix" <<'PYEOF'
import struct
import sys

def make_elf(exprloc, nested):
    # Abbreviation 1: DW_TAG_compile_unit without children, with DW_AT_producer and DW_AT_name, both of form DW_FORM_exprloc.
    # Abbreviation 2: DW_TAG_compile_unit with children and without attributes.
    # Abbreviation 3: DW_TAG_variable without children, with DW_AT_location and DW_AT_name, both of form DW_FORM_exprloc.
    abbrev = bytes([0x01, 0x11, 0x00, 0x25, 0x18, 0x03, 0x18, 0x00, 0x00,
                    0x02, 0x11, 0x01, 0x00, 0x00,
                    0x03, 0x34, 0x00, 0x02, 0x18, 0x03, 0x18, 0x00, 0x00,
                    0x00])
    header = bytes([0x05, 0x00, 0x01, 0x08, 0, 0, 0, 0])  # version 5, DW_UT_compile, address size 8, abbrev offset 0
    if nested:
        body = header + bytes([0x02, 0x03]) + exprloc + bytes([0x01, 0x30, 0x00])
    else:
        body = header + bytes([0x01]) + exprloc + bytes([0x01, 0x30])
    info = struct.pack('<I', len(body)) + body
    shstr = b'\x00.debug_abbrev\x00.debug_info\x00.shstrtab\x00'
    eh_size, ph_size, sh_size = 64, 56, 64
    abbrev_off = eh_size + ph_size
    info_off = abbrev_off + len(abbrev)
    shstr_off = info_off + len(info)
    sh_off = (shstr_off + len(shstr) + 7) & ~7
    total = sh_off + 4 * sh_size
    eh = b'\x7fELF' + bytes([2, 1, 1, 0]) + b'\x00' * 8 + struct.pack('<HHIQQQIHHHHHH', 2, 0xB7, 1, 0, eh_size, sh_off, 0, eh_size, ph_size, 1, sh_size, 4, 3)
    ph = struct.pack('<IIQQQQQQ', 1, 5, 0, 0, 0, total, total, 0x1000)
    section = lambda name, type, off, size: struct.pack('<IIQQQQIIQQ', name, type, 0, 0, off, size, 0, 0, 1, 0)
    b = bytearray(total)
    b[0:eh_size] = eh
    b[eh_size:eh_size + ph_size] = ph
    b[abbrev_off:abbrev_off + len(abbrev)] = abbrev
    b[info_off:info_off + len(info)] = info
    b[shstr_off:shstr_off + len(shstr)] = shstr
    b[sh_off:sh_off + sh_size] = section(0, 0, 0, 0)
    b[sh_off + sh_size:sh_off + 2 * sh_size] = section(1, 1, abbrev_off, len(abbrev))
    b[sh_off + 2 * sh_size:sh_off + 3 * sh_size] = section(15, 1, info_off, len(info))
    b[sh_off + 3 * sh_size:sh_off + 4 * sh_size] = section(27, 3, shstr_off, len(shstr))
    return bytes(b)

valid = bytes([0x01, 0x30])  # length 1: DW_OP_lit0
invalid = bytes([0xFF, 0xFF, 0xFF, 0xFF, 0x0F, 0x30])
wraparound = bytes([0xFF] * 9 + [0x01, 0x30])
for nested in [False, True]:
    suffix = '_nested' if nested else ''
    open(sys.argv[1] + suffix + '_valid.elf', 'wb').write(make_elf(valid, nested))
    open(sys.argv[1] + suffix + '_invalid.elf', 'wb').write(make_elf(invalid, nested))
    open(sys.argv[1] + suffix + '_wraparound.elf', 'wb').write(make_elf(wraparound, nested))
PYEOF

for kind in '' '_nested'; do
    $CLICKHOUSE_LOCAL -q "SELECT tag, attr_name, attr_form FROM file('${prefix}${kind}_valid.elf', DWARF) ARRAY JOIN attr_name, attr_form"
    $CLICKHOUSE_LOCAL -q "SELECT count() FROM file('${prefix}${kind}_invalid.elf', DWARF)" 2>&1 | grep -o -m1 'CANNOT_PARSE_DWARF'
    $CLICKHOUSE_LOCAL -q "SELECT count() FROM file('${prefix}${kind}_wraparound.elf', DWARF)" 2>&1 | grep -o -m1 'CANNOT_PARSE_DWARF'
    rm -f "${prefix}${kind}_valid.elf" "${prefix}${kind}_invalid.elf" "${prefix}${kind}_wraparound.elf"
done
