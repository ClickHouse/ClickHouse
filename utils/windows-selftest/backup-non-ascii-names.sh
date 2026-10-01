#!/usr/bin/env bash
# `BACKUP` and `RESTORE` to a destination whose name is not ASCII, checked from the outside.
#
# On Windows `std::filesystem::path` converts a narrow string through the process's active code
# page, not as UTF-8, so a backup root that enters `std::filesystem` that way is written under a
# different name than the one in the query - and a round trip through the same mangling still
# succeeds, so `RESTORE` alone cannot tell. The check that can is the name of the directory on the
# host. See `pathFromString` in base/base/pathToString.h and src/Backups/BackupIO_*.cpp.
#
# Usage: backup-non-ascii-names.sh <work dir on the host> <the same dir as the binary sees it> <command...>
#   Linux: backup-non-ascii-names.sh /tmp/w /tmp/w clickhouse
#   Wine:  backup-non-ascii-names.sh /tmp/w "$(winepath -w /tmp/w)" wine clickhouse.exe
#
# The SQL goes in on stdin, not in `--query`: the Windows port does not decode the command line as
# UTF-8, so a non-ASCII argument would be mangled before it reached the backup code.

set -eu

host_dir=$1
binary_dir=$2
shift 2

rm -rf "$host_dir"
mkdir -p "$host_dir/data" "$host_dir/backups"

result=$("$@" local --path "$binary_dir/data" -- --backups.allowed_disk=default "--backups.allowed_path=$binary_dir/backups" <<'EOF'
CREATE TABLE t (x UInt64, s String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT number, toString(number) FROM numbers(100);
BACKUP TABLE t TO Disk('default', 'бэкап_диск') FORMAT Null;
BACKUP TABLE t TO File('бэкап_файл') FORMAT Null;
RESTORE TABLE t AS t_from_disk FROM Disk('default', 'бэкап_диск') FORMAT Null;
RESTORE TABLE t AS t_from_file FROM File('бэкап_файл') FORMAT Null;
SELECT count(), sum(x), sum(toUInt64(s)) FROM t_from_disk;
SELECT count(), sum(x), sum(toUInt64(s)) FROM t_from_file;
EOF
)

failures=0

expected=$'100\t4950\t4950\n100\t4950\t4950'
if [ "$result" != "$expected" ]
then
    echo "FAILED: the restored tables differ from the backed up one, got:"
    echo "$result"
    failures=$((failures + 1))
fi

for dir in "$host_dir/data/бэкап_диск" "$host_dir/backups/бэкап_файл"
do
    if [ ! -f "$dir/.backup" ]
    then
        echo "FAILED: no backup under its UTF-8 name $dir, found instead:"
        ls -la "$(dirname "$dir")"
        failures=$((failures + 1))
    fi
done

[ "$failures" -eq 0 ] && echo "OK"
exit "$failures"
