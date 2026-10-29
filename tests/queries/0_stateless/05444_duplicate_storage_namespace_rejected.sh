#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database
# Tag no-replicated-database: the disks below are not shared between replicas

# Two writable disks over one and the same storage make `MergeTree` enumerate every part twice. It then
# removes one copy as a duplicate, which deletes the data. Registering the second disk must fail.
#
# Both disks keep their metadata in the same place as well, because a disk that stores metadata locally
# reports that local path as its own path. Two disks over one object storage but with a metadata path
# each are two namespaces, not one.

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

# A second writable disk over the same storage: rejected.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE second (s String) ORDER BY ()
SETTINGS disk = disk(
      name = 05444_second_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = local,
      metadata_path = '${metadata_path}',
      path = '${objects_path}')
" 2>&1 | grep -o -F "resolve to the same storage namespace" | head -n 1

# The writer keeps working, and keeps its data.
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer VALUES ('Hello')"
${CLICKHOUSE_CLIENT} --query "SELECT * FROM writer"

${CLICKHOUSE_CLIENT} --query "DROP TABLE writer SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS second SYNC"
