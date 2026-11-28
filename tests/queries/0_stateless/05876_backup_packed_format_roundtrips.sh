#!/usr/bin/env bash
# Tags: no-fasttest
# ^ backups need a running server with a configured 'backups' disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# backup_use_packed_format bundles many small backup files into a few pack objects. Row counts are
# tiny on purpose: the number of data files, hence of packs, follows the schema and not the row count, so a
# few dozen rows exercises every path.

name="${CLICKHOUSE_TEST_UNIQUE_NAME}"
backups_disk_path="$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name='backups'")"

checksum() { ${CLICKHOUSE_CLIENT} --query "SELECT sum(cityHash64(*)) FROM $1"; }

roundtrip() {
    local label=$1 backup=$2 create=$3 insert=$4 backup_settings=$5
    ${CLICKHOUSE_CLIENT} -m --query "DROP TABLE IF EXISTS t; $create"
    ${CLICKHOUSE_CLIENT} --query "$insert"
    local before after
    before=$(checksum t)
    ${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO $backup $backup_settings" | grep -o BACKUP_CREATED
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
    ${CLICKHOUSE_CLIENT} --query "RESTORE TABLE t FROM $backup" | grep -o RESTORED
    after=$(checksum t)
    if [ "$before" = "$after" ]; then echo "$label identical"; else echo "$label MISMATCH $before != $after"; fi
}

# min_bytes_for_full_part_storage=0 keeps a part one file per column: packed part storage would put the whole part
# in a single data.packed file, leaving one data file to pack and nothing to deduplicate.
part_settings="min_bytes_for_wide_part=0, min_bytes_for_full_part_storage=0"
small_create="CREATE TABLE t (a UInt64, s String) ENGINE=MergeTree ORDER BY a SETTINGS $part_settings"
small_insert="INSERT INTO t SELECT number, toString(number) FROM numbers(50)"

# Case 1: mixed small + big; big files stay their own object next to the packs, which only this case writes.
roundtrip "mixed" "Disk('backups', '${name}_1')" \
    "CREATE TABLE t (a UInt64, s String) ENGINE=MergeTree ORDER BY a SETTINGS $part_settings" \
    "INSERT INTO t SELECT number, repeat('x', 200) FROM numbers(50)" \
    "SETTINGS backup_use_packed_format=1, backup_pack_min_size=200"

# Case 2: small files spilling across a few packs. pack_size is chosen just under the packable total so it
# spills into 2-3 packs (kept tiny on purpose) -- then assert >1 pack actually formed.
roundtrip "multi-pack" "Disk('backups', '${name}_2')" "$small_create" \
    "$small_insert" \
    "SETTINGS backup_use_packed_format=1, backup_pack_size=800"
npacks=$(ls "${backups_disk_path}${name}_2"/packs_* 2>/dev/null | wc -l)
if [ "$npacks" -gt 1 ]; then echo "multi-pack spilled"; else echo "multi-pack ONLY $npacks pack(s)"; fi

# Case 3: duplicated data (two identical columns) -> members collapse via dedup.
roundtrip "dedup" "Disk('backups', '${name}_3')" \
    "CREATE TABLE t (a UInt64, b UInt64) ENGINE=MergeTree ORDER BY tuple() SETTINGS $part_settings" \
    "INSERT INTO t SELECT number, number FROM numbers(50)" \
    "SETTINGS backup_use_packed_format=1"
# A duplicate of a packed member must not write a loose own-object (it would land nested under data/...),
# so the backup dir holds nothing but the manifest and the packs.
find "${backups_disk_path}${name}_3" -type f ! -name '.backup' ! -name 'packs_*' | wc -l

# Case 4: packed accounting. A pack is one entry whose size includes the front index, so packing gives fewer entries
# and a larger total size.
${CLICKHOUSE_CLIENT} -m --query "DROP TABLE IF EXISTS t; $small_create"
${CLICKHOUSE_CLIENT} --query "$small_insert"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_4off') SETTINGS id='${name}_4off'" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t TO Disk('backups', '${name}_4on') SETTINGS id='${name}_4on', backup_use_packed_format=1" | grep -o BACKUP_CREATED
${CLICKHOUSE_CLIENT} --query "
SELECT
    (SELECT num_entries FROM system.backups WHERE id='${name}_4on') <
    (SELECT num_entries FROM system.backups WHERE id='${name}_4off') AS packed_has_fewer_entries,
    (SELECT uncompressed_size FROM system.backups WHERE id='${name}_4on') >
    (SELECT uncompressed_size FROM system.backups WHERE id='${name}_4off') AS packed_size_includes_index"

# Case 5: fsync_backup_files covers the packs. Each stored object -- an own object or a pack, one entry
# each in system.backups -- is fsynced once, plus the .backup manifest (the oracle of 04412_backup_to_file_fsync).
${CLICKHOUSE_CLIENT} -m --query "DROP TABLE IF EXISTS t; $small_create"
${CLICKHOUSE_CLIENT} --query "$small_insert"
for dest in "Disk('backups', '${name}_5disk')" "File('${name}_5file')"; do
    qid="${name}_5_$(echo "$dest" | cut -c1-4)"
    ${CLICKHOUSE_CLIENT} --query_id "$qid" --format Null \
        --query "BACKUP TABLE t TO $dest SETTINGS backup_use_packed_format=1, backup_pack_size=800, fsync_backup_files=1"
    ${CLICKHOUSE_CLIENT} -m --query "
        SYSTEM FLUSH LOGS query_log;
        SELECT '$(echo "$dest" | cut -c1-4) every_object_synced', q.ProfileEvents['FileSync'] = b.num_entries + 1
        FROM system.query_log AS q JOIN system.backups AS b ON q.query_id = b.query_id
        WHERE q.type = 'QueryFinish' AND q.current_database = currentDatabase() AND q.query_id = '$qid'"
done

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS t"
