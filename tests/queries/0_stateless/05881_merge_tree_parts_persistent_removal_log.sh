#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

# A disk with `local` metadata and `persistent_removal_log` loads, when it starts, the queue of removed
# blobs that was persisted next to the metadata, and removes them from the object storage in background.
# The disk of `mergeTreeParts` exists only to read, so a description with that key is rejected before
# the disk is created: nothing is started, and neither the queue nor its objects are touched.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_DISKS_FILES}/mtp_removal_log_${CLICKHOUSE_DATABASE}/"
DISK="type = object_storage, object_storage_type = local, metadata_type = local, path = '${ROOT}data/', metadata_path = '${ROOT}metadata/'"

function run()
{
    ${CLICKHOUSE_CLIENT} --query "
        $1 mergeTreeParts(
            structure('x UInt8'),
            parts(),
            disk($2),
            table_settings(index_granularity_bytes = 10485760))
        FORMAT TSV" 2>&1 | cut -f 1,2 | grep -o "BAD_ARGUMENTS\|^x.*\|^[0-9]*$" | head -1
}

echo "-- with a persistent removal log"
run "SELECT count() FROM" "${DISK}, persistent_removal_log = 1"
run "DESCRIBE" "${DISK}, persistent_removal_log = 1"
run "SELECT count() FROM" "${DISK}, persistent_removal_log = 0"
test -e "${ROOT}" && echo "the disk was created" || echo "nothing was created"

echo "-- without it"
run "SELECT count() FROM" "${DISK}"
run "DESCRIBE" "${DISK}"
test -e "${ROOT}metadata/.metadata" && echo "the removal log directory was created" || echo "no removal log directory"

rm -rf "${ROOT}"
