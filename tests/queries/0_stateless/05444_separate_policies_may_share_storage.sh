#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database
# Tag no-replicated-database: the disks below are not shared between replicas

# Two writable disks over one and the same storage are a danger only to the table that reads through
# both of them: a storage policy holding both makes `MergeTree` enumerate every part twice and remove
# one copy as a duplicate, which deletes the data. `StoragePolicy` rejects that pair.
#
# A disk is created long before it is known which policy will hold it, so registering two disks over one
# storage must stay allowed. Each table below has a policy of its own with a single disk in it, so neither
# table ever sees a part twice. ClickHouse's own test configuration relies on this: two `cache` disks over
# one local disk, each in its own single-disk policy.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS writer SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS second SYNC"

objects_path="${CLICKHOUSE_DISKS_FILES}/05444_objects/${CLICKHOUSE_DATABASE}/"
metadata_path="${CLICKHOUSE_DISKS_FILES}/05444_metadata/${CLICKHOUSE_DATABASE}/"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE writer (s String) ORDER BY ()
SETTINGS disk = disk(
      name = 05444_writer_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = local,
      metadata_path = '${metadata_path}',
      path = '${objects_path}')
"

# A second writable disk over the same storage, in a policy of its own: accepted.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE second (s String) ORDER BY ()
SETTINGS disk = disk(
      name = 05444_second_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = local,
      metadata_path = '${metadata_path}',
      path = '${objects_path}')
"

# Both tables work and keep their own data.
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer VALUES ('Hello')"
${CLICKHOUSE_CLIENT} --query "INSERT INTO second VALUES ('World')"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM writer"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM second"

${CLICKHOUSE_CLIENT} --query "DROP TABLE writer SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE second SYNC"
