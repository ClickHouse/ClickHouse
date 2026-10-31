#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database
# Tag no-replicated-database: the disks below are not shared between replicas

# Two `encrypted` disks over one and the same disk are siblings: neither stands on the other, and both
# write the same files. A storage policy holding both is rejected, because the table would see every part
# twice. Separately, each of them in a policy of its own is a supported setup and must register.
#
# A `cache` disk beside another `cache` disk over one local disk is the same shape, and ClickHouse's own
# test configuration declares exactly that in `storage_conf_local.xml`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS sibling_first SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS sibling_second SYNC"

inner_disk="05445_inner_${CLICKHOUSE_DATABASE}"
objects_path="${CLICKHOUSE_DISKS_FILES}/05445_objects/${CLICKHOUSE_DATABASE}/"
metadata_path="${CLICKHOUSE_DISKS_FILES}/05445_metadata/${CLICKHOUSE_DATABASE}/"

# An `encrypted` disk over a writable object storage disk, with no `path` of its own, so it reports the
# path of the disk it wraps. Both disks are registered, and the layer and the disk below it are not a pair.
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

# A second `encrypted` disk over the same wrapped disk, in a policy of its own: accepted.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE sibling_second (s String) ORDER BY ()
SETTINGS disk = disk(
      name = 05445_encrypted_second_${CLICKHOUSE_DATABASE},
      type = encrypted,
      disk = '${inner_disk}',
      algorithm = 'AES_128_CTR',
      key_hex = 'ffeeddccbbaa99887766554433221100')
"

# Both tables work and keep their own data.
${CLICKHOUSE_CLIENT} --query "INSERT INTO sibling_first VALUES ('Hello')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO sibling_second VALUES ('World')"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM sibling_first"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM sibling_second"

${CLICKHOUSE_CLIENT} --query "DROP TABLE sibling_first SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE sibling_second SYNC"
