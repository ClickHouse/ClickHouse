#!/usr/bin/env bash
# Tags: no-fasttest
# ^ backups need a running server with a configured 'backups' disk.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A Log table's files only grow, so an incremental stores just their new suffixes; with packing on a suffix is a pack
# member and restore joins it to the base's prefix. An untouched MergeTree table's files come whole from the
# base's packs and are copied from there.

name="${CLICKHOUSE_TEST_UNIQUE_NAME}"
backups_disk_path="$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.disks WHERE name='backups'")"

checksum() { ${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(cityHash64(*)) FROM $1"; }

${CLICKHOUSE_CLIENT} -m --query "
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS m;
CREATE TABLE t (a UInt64, s String) ENGINE=Log;
INSERT INTO t SELECT number, toString(number) FROM numbers(50);
CREATE TABLE m (a UInt64, s String) ENGINE=MergeTree ORDER BY a;
INSERT INTO m SELECT number, toString(number) FROM numbers(50);
"
${CLICKHOUSE_CLIENT} --query "BACKUP TABLE t, TABLE m TO Disk('backups', '${name}_base') SETTINGS backup_use_packed_format=1" | grep -o BACKUP_CREATED

${CLICKHOUSE_CLIENT} --query "INSERT INTO t SELECT number, toString(number) FROM numbers(50, 50)"
before_t=$(checksum t)
before_m=$(checksum m)
${CLICKHOUSE_CLIENT} --query "
    BACKUP TABLE t, TABLE m TO Disk('backups', '${name}_incr')
    SETTINGS backup_use_packed_format=1, base_backup=Disk('backups', '${name}_base')" | grep -o BACKUP_CREATED

# The incremental must hold a packed member that continues a file of the base, and MergeTree data files taken whole
# from files packed in the base, or the restore below proves nothing.
python3 - "${backups_disk_path}${name}_base/.backup" "${backups_disk_path}${name}_incr/.backup" <<'PY'
import re, sys
def files(path):
    return re.findall(r"<file>.*?</file>", open(path, encoding="utf-8").read(), re.S)
def name(f):
    return re.search(r"<name>(.*?)</name>", f).group(1)
packed_in_base = {name(f) for f in files(sys.argv[1]) if "<packed>true</packed>" in f}
incr = files(sys.argv[2])
print("packed suffixes:", int(any("<base_size>" in f and "<packed>true</packed>" in f for f in incr)))
print("whole files from packed base:", int(any(
    "/m/" in name(f) and "<use_base>true</use_base>" in f and "<base_size>" not in f and name(f) in packed_in_base
    for f in incr)))
PY

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
${CLICKHOUSE_CLIENT} --query "DROP TABLE m"
restore_and_compare() {
    local table=$1 before=$2
    ${CLICKHOUSE_CLIENT} --query "
        RESTORE TABLE ${table} FROM Disk('backups', '${name}_incr')
        SETTINGS base_backup=Disk('backups', '${name}_base')" | grep -o RESTORED
    if [ "$before" = "$(checksum "$table")" ]; then echo "$table identical"; else echo "$table MISMATCH"; fi
}
restore_and_compare t "$before_t"
restore_and_compare m "$before_m"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t"
${CLICKHOUSE_CLIENT} --query "DROP TABLE m"
