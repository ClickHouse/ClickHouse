#!/usr/bin/env bash

# Copies a test predefined backup from "/tests/queries/0_stateless/backups/" folder to the "backups" disk,
# returns the path to the backup relative to that disk.
#
# Usage:
#     install_predefined_backup.sh <filename_in_backups_folder>

HELPERS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

src_backup_filename="$1"
src_backup_path="$HELPERS_DIR/../backups/${src_backup_filename}"

backups_disk_root=$($CLICKHOUSE_CLIENT --query "SELECT path FROM system.disks WHERE name='backups'")

if [ -z "${backups_disk_root}" ]; then
    echo "Disk 'backups' not found"
    exit 1
fi

dest_relative_path=${CLICKHOUSE_DATABASE}/${src_backup_filename}
dest_path=${backups_disk_root}/${dest_relative_path}

mkdir -p "$(dirname "${dest_path}")"
ln -s "${src_backup_path}" "${dest_path}"

echo "${dest_relative_path}"
