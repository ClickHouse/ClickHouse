#!/usr/bin/env bash
# Tags: no-object-storage, no-replicated-database, no-shared-merge-tree
# Tag no-replicated-database: plain rewritable should not be shared between replicas

# A refresh of a read-only table must drop the parts that the writer removed: their directories are gone,
# so reading them fails.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS writer SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS reader SYNC"

disk_path="${CLICKHOUSE_DISKS_FILES}/05832/${CLICKHOUSE_DATABASE}/"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE writer (x UInt64) ORDER BY x
SETTINGS table_disk = true, old_parts_lifetime = 0, cleanup_delay_period = 0, max_cleanup_delay_period = 1,
  cleanup_delay_period_random_add = 0,
  disk = disk(
      name = 05832_writer_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      metadata_snapshot_write_delay_ms = 0,
      path = '${disk_path}')
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO writer SELECT number FROM numbers(10)"

# Without refresh_parts_interval: the refresh below is the only one.
${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader (x UInt64) ORDER BY x
SETTINGS table_disk = true,
  disk = disk(
      readonly = true,
      name = 05832_reader_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path}')
"

${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM reader"

${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE writer"
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer SELECT number FROM numbers(100, 5)"

# Until the writer has removed everything but the new part (selecting `_state` makes system.parts return all parts).
while clickhouse_test_time_left; do
    [[ $(${CLICKHOUSE_CLIENT} --query "
        SELECT countIf(_state != 'Active') = 0 AND count() = 1 FROM system.parts
        WHERE database = currentDatabase() AND table = 'writer'") == 1 ]] && break
    sleep 0.1
done

${CLICKHOUSE_CLIENT} --query "SYSTEM RESTART DISK 05832_reader_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM reader"
${CLICKHOUSE_CLIENT} --query "
SELECT
    (SELECT groupArraySorted(10)(name) FROM system.parts WHERE database = currentDatabase() AND table = 'reader' AND active)
  = (SELECT groupArraySorted(10)(name) FROM system.parts WHERE database = currentDatabase() AND table = 'writer' AND active)"
# The removed part is forgotten, not kept as an old part.
${CLICKHOUSE_CLIENT} --query "
SELECT _state, count() FROM system.parts
WHERE database = currentDatabase() AND table = 'reader' GROUP BY _state ORDER BY _state"

${CLICKHOUSE_CLIENT} --query "DROP TABLE reader SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE writer SYNC"

# A writer created again on the same path gives its new parts the names of the removed ones.
disk_path_2="${CLICKHOUSE_DISKS_FILES}/05832/${CLICKHOUSE_DATABASE}_2/"
writer2_ddl="
CREATE TABLE writer2 (x UInt64) ORDER BY tuple()
SETTINGS table_disk = true, old_parts_lifetime = 0, cleanup_delay_period = 0, max_cleanup_delay_period = 1,
  cleanup_delay_period_random_add = 0,
  disk = disk(
      name = 05832_writer2_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      metadata_snapshot_write_delay_ms = 0,
      path = '${disk_path_2}')"

${CLICKHOUSE_CLIENT} --query "$writer2_ddl"
${CLICKHOUSE_CLIENT} --query "SYSTEM STOP MERGES writer2"
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer2 SELECT number * 2 FROM numbers(2)"
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer2 SELECT number * 2 FROM numbers(2)"

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE reader2 (x UInt64) ORDER BY tuple()
SETTINGS table_disk = true,
  disk = disk(
      readonly = true,
      name = 05832_reader2_${CLICKHOUSE_DATABASE},
      type = object_storage,
      object_storage_type = local,
      metadata_type = plain_rewritable,
      path = '${disk_path_2}')
"

# The query condition cache remembers that all_1_1_0 has no row with x = 1 (its rows 0 and 2 keep it from being pruned).
# The uncompressed cache is keyed by the file path, which the new all_1_1_0 shares, so it is off.
filtered_count="SELECT count() FROM reader2 PREWHERE x = 1
    SETTINGS use_query_condition_cache = 1, use_skip_indexes = 0, use_uncompressed_cache = 0"
${CLICKHOUSE_CLIENT} --query "
SELECT groupArraySorted(10)(name) FROM system.parts WHERE database = currentDatabase() AND table = 'reader2' AND active"
${CLICKHOUSE_CLIENT} --query "$filtered_count"

${CLICKHOUSE_CLIENT} --query "SYSTEM START MERGES writer2"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE writer2 FINAL"
while clickhouse_test_time_left; do
    [[ $(${CLICKHOUSE_CLIENT} --query "
        SELECT countIf(_state != 'Active') = 0 AND count() = 1 FROM system.parts
        WHERE database = currentDatabase() AND table = 'writer2'") == 1 ]] && break
    sleep 0.1
done
${CLICKHOUSE_CLIENT} --query "SYSTEM RESTART DISK 05832_reader2_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} --query "
SELECT groupArraySorted(10)(name) FROM system.parts WHERE database = currentDatabase() AND table = 'reader2' AND active"

# The new all_1_1_0 is covered by the reader's all_1_2_1, whose directory is gone.
${CLICKHOUSE_CLIENT} --query "DROP TABLE writer2 SYNC"
${CLICKHOUSE_CLIENT} --query "$writer2_ddl"
${CLICKHOUSE_CLIENT} --query "INSERT INTO writer2 SELECT 1 FROM numbers(5)"
${CLICKHOUSE_CLIENT} --query "SYSTEM RESTART DISK 05832_reader2_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM reader2 SETTINGS use_uncompressed_cache = 0"
${CLICKHOUSE_CLIENT} --query "$filtered_count"

${CLICKHOUSE_CLIENT} --query "DROP TABLE reader2 SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE writer2 SYNC"
