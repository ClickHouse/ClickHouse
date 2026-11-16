#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-replicated-database: plain rewritable should not be shared between replicas

# The read-only disk outlives the detached `reader`, so ATTACH must reload its metadata: the parts it listed
# before the DETACH were merged and removed by the writer meanwhile.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS writer SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS reader SYNC"

disk_path="${CLICKHOUSE_DISKS_FILES}/05628/${CLICKHOUSE_DATABASE}/"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE writer (x UInt64) ORDER BY x
SETTINGS table_disk = true, old_parts_lifetime = 0, cleanup_delay_period = 0, max_cleanup_delay_period = 1,
  cleanup_delay_period_random_add = 0,
  disk = disk(
      name = 05628_writer_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      metadata_snapshot_write_delay_ms = 0,
      path = '${disk_path}')
"

for _ in {1..3}; do
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO writer SELECT number FROM numbers(10)"
done

# Without refresh_parts_interval nothing reloads the reader's disk in the background.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader (x UInt64) ORDER BY x
SETTINGS table_disk = true,
  disk = disk(
      readonly = true,
      name = 05628_reader_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path}')
"

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM reader"
${CLICKHOUSE_CLIENT} --query "DETACH TABLE reader SYNC"

${CLICKHOUSE_CLIENT} --query "INSERT INTO writer SELECT number FROM numbers(10)"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE writer FINAL"

# Selecting `_state` makes system.parts return the non-Active parts too.
while clickhouse_test_time_left; do
    [[ $(${CLICKHOUSE_CLIENT} --query "
        SELECT countIf(_state != 'Active') FROM system.parts
        WHERE database = currentDatabase() AND table = 'writer'") == 0 ]] && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "ATTACH TABLE reader"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM reader"

${CLICKHOUSE_CLIENT} --query "DROP TABLE reader SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE writer SYNC"
