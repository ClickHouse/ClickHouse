#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# no-shared-merge-tree: custom disk

# `DESCRIBE` of `mergeTreeParts` never creates the disk, but it must reject a local disk configuration
# that the disk creator rejects as well, such as a `path` without the trailing `/`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_DISKS_FILES}/mtp_describe_local_config_${CLICKHOUSE_DATABASE}"

# Prints the structure for an accepted description, or the error code for a rejected one.
function describe()
{
    ${CLICKHOUSE_CLIENT} --query "
        DESCRIBE mergeTreeParts(
            structure('x UInt8'),
            parts(),
            disk($1),
            table_settings(index_granularity_bytes = 10485760))
        FORMAT TSV" 2>&1 | grep -o "UNKNOWN_ELEMENT_IN_CONFIG\|EXCESSIVE_ELEMENT_IN_CONFIG\|^x.*" | head -1
}

echo "-- a local disk with the trailing slash"
describe "type = local, path = '${ROOT}/local/'"

echo "-- a local disk without the trailing slash"
describe "type = local, path = '${ROOT}/local'"

echo "-- a local object storage without the trailing slash"
describe "type = object_storage, object_storage_type = local, metadata_type = plain, path = '${ROOT}/object'"

echo "-- both keep_free_space_bytes and keep_free_space_ratio"
describe "type = local, path = '${ROOT}/local/', keep_free_space_bytes = 0, keep_free_space_ratio = 0.1"
