#!/usr/bin/env bash
# Tags: no-fasttest
# ^ backups need a running server with a configured 'backups' disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A packed backup whose manifest or pack indexes were tampered with must fail closed on restore.

name="${CLICKHOUSE_TEST_UNIQUE_NAME}"
backups_disk_path="$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name='backups'")"

small_create="CREATE TABLE t (a UInt64, s String) ENGINE=MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part=0"
small_insert="INSERT INTO t SELECT number, toString(number) FROM numbers(50)"

# Case 1: num_packs > 0 in an archive backup is corruption (archives are never packed on write): RESTORE must report BACKUP_DAMAGED.
${CLICKHOUSE_CLIENT} -m --query "DROP TABLE IF EXISTS t; $small_create"
${CLICKHOUSE_CLIENT} --query "$small_insert"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_1.zip')" | grep -o BACKUP_CREATED
python3 - "${backups_disk_path}${name}_1.zip" <<'PY'
import sys, zipfile, os
path = sys.argv[1]
tmp = path + ".tmp"
with zipfile.ZipFile(path, "r") as zin, zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zout:
    for item in zin.infolist():
        data = zin.read(item.filename)
        if item.filename == ".backup":
            data = data.decode("utf-8").replace("<contents>", "<num_packs>1</num_packs><contents>", 1).encode("utf-8")
        zout.writestr(item, data)
os.replace(tmp, path)
PY
${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t FROM Disk('backups', '${name}_1.zip')" 2>&1 \
    | grep -o "Archive backup cannot contain packed objects" | head -1

# Case 2: restore must reject any disagreement between the manifest's <packed> markers and the pack indexes (membership
# or byte ranges): a dropped member would read as an own object of the same data_file_name. Each case tampers with a
# copy of one good backup.
${CLICKHOUSE_CLIENT} -m --query "DROP TABLE IF EXISTS t; $small_create"
${CLICKHOUSE_CLIENT} --query "$small_insert"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_2') SETTINGS backup_use_packed_format=1" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "DROP TABLE t"

restore_tampered() {
    local mode=$1
    local dir="${backups_disk_path}${name}_2_${mode}"
    rm -rf "$dir"
    cp -r "${backups_disk_path}${name}_2" "$dir"
    python3 - "$dir" "$mode" <<'PY'
import glob, os, re, struct, sys

directory, mode = sys.argv[1], sys.argv[2]
manifest_path = os.path.join(directory, ".backup")

if mode not in ("drop_index_member", "shrink_index_member"):
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = f.read()
    if mode == "drop_num_packs":
        manifest = re.sub(r"<num_packs>\d+</num_packs>", "", manifest, count=1)
    elif mode == "downgrade_version":
        manifest = re.sub(r"<version>\d+</version>", "<version>1</version>", manifest, count=1)
    else:
        manifest = manifest.replace("<packed>true</packed>", "")
    with open(manifest_path, "w", encoding="utf-8") as f:
        f.write(manifest)
    sys.exit(0)

pack_path = sorted(glob.glob(os.path.join(directory, "packs_*")))[0]
with open(pack_path, "rb") as f:
    pack = f.read()

def read_varint(pos):
    value = shift = 0
    while True:
        byte = pack[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7

def write_varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        out.append(byte | 0x80 if value else byte)
        if not value:
            return bytes(out)

version = pack[0]
entry_format = "<QQQ" if version >= 1 else "<QQ"
entry_size = struct.calcsize(entry_format)
(count,) = struct.unpack_from("<Q", pack, 1)
pos = 9
members = []
for _ in range(count):
    length, pos = read_varint(pos)
    name = pack[pos:pos + length]
    pos += length
    members.append((name, struct.unpack_from(entry_format, pack, pos)))
    pos += entry_size

if mode == "drop_index_member":
    # Drop the first member. Bodies keep their absolute offsets, and the shortened index is zero-padded back to
    # its original length, so every surviving member still resolves -- only the dropped one goes missing.
    members = members[1:]
else:
    # Understate the first member's recorded size by one byte. Every field keeps its width, so the index still
    # parses and every member still resolves -- the only damage is the disagreement with the manifest.
    name, entry = members[0]
    members[0] = (name, entry[:1] + (entry[1] - 1,) + entry[2:])

index = bytearray(pack[:1]) + struct.pack("<Q", len(members))
for name, entry in members:
    index += write_varint(len(name)) + name + struct.pack(entry_format, *entry)
index += b"\0" * (pos - len(index))
with open(pack_path, "wb") as f:
    f.write(bytes(index) + pack[pos:])
PY
    ${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t FROM Disk('backups', '${name}_2_${mode}')" 2>&1 \
        | grep -oE "marked as packed in the metadata but no pack contains it|is not marked as packed in the metadata|predates the packed format|in the pack index but the metadata declares" | head -1
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t"
}

restore_tampered drop_index_member
restore_tampered shrink_index_member
# Without <num_packs> no pack index is loaded, so only the manifest's <packed> markers can catch it.
restore_tampered drop_num_packs
restore_tampered drop_markers
restore_tampered downgrade_version

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t"
