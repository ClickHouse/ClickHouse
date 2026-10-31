#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database
# Tag no-replicated-database: the disks below are not shared between replicas

# An `encrypted` disk stands on the disk it wraps and reports that disk's path as its own, so the pair
# is one storage behind one metadata storage and registering both is allowed.
#
# Two `encrypted` disks over one and the same disk are a different matter: neither stands on the other,
# and both write the same files. A storage policy built from both makes `MergeTree` enumerate every part
# twice and remove one copy as a duplicate, which deletes the data. Registering the second one must fail.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS sibling_first SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS sibling_second SYNC"

inner_disk="05445_inner_${CLICKHOUSE_DATABASE}"
objects_path="${CLICKHOUSE_DISKS_FILES}/05445_objects/${CLICKHOUSE_DATABASE}/"
metadata_path="${CLICKHOUSE_DISKS_FILES}/05445_metadata/${CLICKHOUSE_DATABASE}/"

# An `encrypted` disk over a writable object storage disk, with no `path` of its own: it reports the path
# of the disk it wraps. The wrapped disk is registered under its name as well, and the two are a layer and
# the disk below it, so both are accepted.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE sibling_first (s String) ORDER BY ()
SETTINGS disk = disk(
      name = 05445_encrypted_first_${CLICKHOUSE_DATABASE},
      type = encrypted,
      disk = disk(
          name = ${inner_disk},
          type = object_storage,
          object_storage_type = local,
          metadata_type = local,
          metadata_path = '${metadata_path}',
          path = '${objects_path}'),
      algorithm = 'AES_128_CTR',
      key_hex = '00112233445566778899aabbccddeeff')
"

# A second `encrypted` disk over the same wrapped disk. A different key does not give it a storage of its
# own - it writes the same files - so it is a duplicate of the first one and is rejected.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE sibling_second (s String) ORDER BY ()
SETTINGS disk = disk(
      name = 05445_encrypted_second_${CLICKHOUSE_DATABASE},
      type = encrypted,
      disk = '${inner_disk}',
      algorithm = 'AES_128_CTR',
      key_hex = 'ffeeddccbbaa99887766554433221100')
" 2>&1 | grep -o -F "resolve to the same storage namespace" | head -n 1

# The first disk keeps working, and keeps its data.
${CLICKHOUSE_CLIENT} --query "INSERT INTO sibling_first VALUES ('Hello')"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM sibling_first"

${CLICKHOUSE_CLIENT} --query "DROP TABLE sibling_first SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS sibling_second SYNC"
