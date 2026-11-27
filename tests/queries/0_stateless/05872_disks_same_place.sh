#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# Tag no-fasttest: custom disks are not configured in fasttest, and an `encrypted` disk needs OpenSSL
# Tag no-replicated-database, no-shared-merge-tree: custom disk

# Two separate disks cannot store data in the same place. Layers over one disk can.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

root="${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
key="key = '1234567812345678'"

function create()
{
    local table=$1 disk=$2
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${table} (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS disk = ${disk}" 2>&1 \
        | grep -o -m1 'store data in the same place'
}

echo '-- a local disk'
create t_local "disk(type = local, path = '${root}/one/')"

echo '-- another local disk in the same directory'
create t_local_again "disk(type = local, path = '${root}/one/', keep_free_space_bytes = 1)"

echo '-- the same directory spelled differently'
create t_local_spelling "disk(type = local, path = '${root}/one/./')"

echo '-- a directory whose name differs only by a dot at the end'
create t_local_dot "disk(type = local, path = '${root}/one./')"

echo '-- an encrypted disk in the directory of another local disk'
create t_inner "disk(type = local, path = '${root}/two/enc/')"
create t_outer "disk(type = encrypted, path = 'enc/', ${key}, disk = disk(type = local, path = '${root}/two/'))"

echo '-- layers over one disk in the same directory'
create t_layer_1 "disk(type = encrypted, ${key}, disk = disk(type = local, path = '${root}/one/'))"
create t_layer_2 "disk(type = encrypted, path = 'enc/', ${key}, disk = disk(type = local, path = '${root}/three/'))"
create t_layer_3 "disk(type = encrypted, path = 'enc/', key = '8765432187654321', disk = disk(type = local, path = '${root}/three/'))"

echo '-- a local disk inside the directory of another one'
create t_nested "disk(type = local, path = '${root}/one/nested/')"

echo '-- plain_rewritable disks with one path and different compatibility prefixes'
plain="type = object_storage, object_storage_type = local, metadata_type = plain_rewritable, path = '${root}/plain/'"
create t_plain "disk(${plain}, key_compatibility_prefix = '${root}/plain/a/')"
create t_plain_again "disk(${plain}, key_compatibility_prefix = '${root}/plain/b/')"

${CLICKHOUSE_CLIENT} --query "SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name"
