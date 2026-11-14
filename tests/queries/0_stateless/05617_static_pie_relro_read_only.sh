#!/usr/bin/env bash
# Tags: linux

# The `PT_GNU_RELRO` range of the executable (GOT, relocated read-only data) must be read-only
# at runtime, not only marked in the ELF file: a static PIE has no dynamic linker to apply it,
# so the startup code does it. Compares the range from the program headers with the mappings
# of a running `clickhouse local`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

binary=$(readlink -f "$CLICKHOUSE_BINARY")

${CLICKHOUSE_LOCAL} -q "SELECT line FROM file('/proc/self/maps', LineAsString)" | python3 -c '
import os, struct, sys

binary = sys.argv[1]
with open(binary, "rb") as f:
    header = f.read(64)
    assert header[:4] == b"\x7fELF" and header[4] == 2, "expected a 64-bit ELF"
    phoff, = struct.unpack_from("<Q", header, 32)
    phentsize, phnum = struct.unpack_from("<HH", header, 54)
    f.seek(phoff)
    phdrs = f.read(phentsize * phnum)

page = os.sysconf("SC_PAGE_SIZE")
relro = []
first_load = None
for i in range(phnum):
    p_type, _, p_offset, p_vaddr, _, _, p_memsz, _ = struct.unpack_from("<IIQQQQQQ", phdrs, i * phentsize)
    if p_type == 1 and first_load is None:  # PT_LOAD
        first_load = (p_offset & -page, p_vaddr & -page)
    if p_type == 0x6474E552:  # PT_GNU_RELRO
        start = p_vaddr & -page
        end = (p_vaddr + p_memsz) & -page
        if end > start:
            relro.append((start, end))
print("has RELRO:", len(relro) == 1)

# Mappings of the executable as (start, end, permissions, file offset); the load bias comes from
# the mapping of the first PT_LOAD segment. RELRO pages are compared by address, not by file
# offset: a segment can be mapped at a different offset from its address.
binary_stat = os.stat(binary)
mappings = []
for line in sys.stdin:
    fields = line.split()
    if len(fields) < 6:
        continue
    major, minor = (int(x, 16) for x in fields[3].split(":"))
    if int(fields[4]) != binary_stat.st_ino or os.makedev(major, minor) != binary_stat.st_dev:
        continue
    lo, hi = (int(x, 16) for x in fields[0].split("-"))
    mappings.append((lo, hi, fields[1], int(fields[2], 16)))
bias = min(lo for lo, _, _, offset in mappings if offset == first_load[0]) - first_load[1]

covered = 0
for lo, hi, permissions, _ in mappings:
    for start, end in relro:
        overlap = min(end + bias, hi) - max(start + bias, lo)
        if overlap > 0:
            covered += overlap
            if permissions != "r--p":
                print("RELRO mapped as", permissions)
print("RELRO fully mapped:", covered == sum(end - start for start, end in relro))
' "$binary"
