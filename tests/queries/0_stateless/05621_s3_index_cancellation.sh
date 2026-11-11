#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-shared-merge-tree
# no-fasttest: requires S3, RocksDB and USearch.
# no-parallel: pauses server-wide index loading and rebuild failpoints.
# no-shared-merge-tree: exercises local part indexes and `Ordinary` metadata.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

database="${CLICKHOUSE_DATABASE}_index_cancellation"
legacy_database="${database}_legacy"
# shellcheck source=./s3_cancellation.lib
. "$CUR_DIR"/s3_cancellation.lib

failpoints=(check_consistency_before_marks_read check_consistency_force_marks_read
            vector_similarity_index_before_read unique_key_sst_before_read
            unique_key_before_index_rebuild unique_key_sst_force_corruption)

cleanup()
{
    disable_failpoints >/dev/null 2>&1 ||:
    if [[ -n "$client_pid" ]]; then
        query --query "KILL QUERY WHERE query_id='${query_id}' ASYNC FORMAT Null" >/dev/null 2>&1 ||:
        wait "$client_pid" ||:
    fi
    for table in consistency sst; do
        query --query "ATTACH TABLE IF NOT EXISTS ${database}.${table}" >/dev/null 2>&1 ||:
    done
    query --query "ATTACH TABLE IF NOT EXISTS ${legacy_database}.rebuild" >/dev/null 2>&1 ||:
    query --query "DROP DATABASE IF EXISTS ${legacy_database} SYNC; DROP DATABASE IF EXISTS ${database} SYNC" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

# Exercise cancellation inside the index readers, before any asynchronous prefetch.
read_options=(--enable_unique_key=1 --max_read_buffer_size=4096 --max_read_buffer_size_remote_fs=4096
              --remote_filesystem_read_method=read --remote_filesystem_read_prefetch=0
              --allow_prefetched_read_pool_for_remote_filesystem=0 --use_uncompressed_cache=0
              --max_parallel_replicas=1 --enable_parallel_replicas=0)
part_settings="storage_policy='s3_no_cache', min_bytes_for_wide_part=0, min_rows_for_wide_part=0,
    min_bytes_for_full_part_storage=0, min_rows_for_full_part_storage=0, min_level_for_full_part_storage=0,
    prewarm_mark_cache=0, prewarm_primary_key_cache=0"

# Force the marks check inside the diagnostic exception handler without removing checksums.
query --query "
    CREATE DATABASE ${database} ENGINE=Atomic SETTINGS disk='default';
    CREATE TABLE ${database}.consistency (n UInt64) ENGINE=MergeTree ORDER BY n SETTINGS ${part_settings};
    SYSTEM STOP MERGES ${database}.consistency;
    INSERT INTO ${database}.consistency SELECT number FROM numbers(4096) SETTINGS max_insert_threads=1, max_block_size=65536, min_insert_block_size_rows=65536, min_insert_block_size_bytes=0;
"
consistency_before=$(state consistency)
query --query "DETACH TABLE ${database}.consistency SYNC; SYSTEM DROP MARK CACHE;
    SYSTEM ENABLE FAILPOINT check_consistency_force_marks_read"
cancel_at "ATTACH TABLE ${database}.consistency" check_consistency_before_marks_read timeout
query --query "SYSTEM DISABLE FAILPOINT check_consistency_force_marks_read;
    ATTACH TABLE IF NOT EXISTS ${database}.consistency"
assert_preserved consistency "$consistency_before"
echo 'consistency cancellation checked'

# Final pipeline cancellation can replace the client error; also check errors created by this query.
assert_no_index_error()
{
    local errors
    errors=$(query --query "SELECT name, last_error_message FROM system.errors
        WHERE query_id='${query_id}' AND name IN ('INCORRECT_DATA', 'CANNOT_OPEN_FILE', 'ROCKSDB_ERROR')")
    if [[ -n "$errors" ]]; then
        printf 'Cancellation was classified as an index error: %s\n' "$errors"
        return 1
    fi
}

# A cold vector index must retain the exception thrown by its input callback.
query --query "
    CREATE TABLE ${database}.vectors (n UInt64, v Array(Float32),
        INDEX idx v TYPE vector_similarity('hnsw', 'L2Distance', 32) GRANULARITY 1000000)
    ENGINE=MergeTree ORDER BY n SETTINGS ${part_settings}, min_compress_block_size=512, max_compress_block_size=4096;
    SYSTEM STOP MERGES ${database}.vectors;
    INSERT INTO ${database}.vectors SELECT number, arrayMap(i -> toFloat32(cityHash64(number, i) % 1000000), range(32)) FROM numbers(4096) SETTINGS max_insert_threads=1, max_block_size=65536, min_insert_block_size_rows=65536, min_insert_block_size_bytes=0;
"
vector_query="SELECT n FROM ${database}.vectors ORDER BY L2Distance(v, arrayMap(i -> toFloat32(cityHash64(toUInt64(0), i) % 1000000), range(32))) LIMIT 1 SETTINGS force_data_skipping_indices='idx'"
vector_result=$(query --query "$vector_query")
[[ -n "$vector_result" ]]
query --query "SYSTEM DROP VECTOR SIMILARITY INDEX CACHE; SYSTEM DROP INDEX UNCOMPRESSED CACHE"
cancel_at "$vector_query" vector_similarity_index_before_read kill
assert_no_index_error
[[ $(query --query "$vector_query") == "$vector_result" ]]
echo 'vector cancellation checked'

# Valid SSTs must not turn cancellation into a RocksDB or index-corruption error.
query --query "
    CREATE TABLE ${database}.sst (n UInt64) ENGINE=MergeTree UNIQUE KEY (n) ORDER BY n SETTINGS ${part_settings};
    INSERT INTO ${database}.sst SELECT number FROM numbers(4096) SETTINGS max_insert_threads=1, max_block_size=65536, min_insert_block_size_rows=65536, min_insert_block_size_bytes=0;
"
sst_before=$(state sst)
query --query "DETACH TABLE ${database}.sst SYNC"
cancel_at "ATTACH TABLE ${database}.sst" unique_key_sst_before_read timeout
query --query "ATTACH TABLE IF NOT EXISTS ${database}.sst"
assert_preserved sst "$sst_before"
echo 'SST validation cancellation checked'

# Cancellation while probing an existing SST must not publish the new rows.
cancel_at "INSERT INTO ${database}.sst SELECT number FROM numbers(4096)" unique_key_sst_before_read kill
assert_no_index_error
assert_preserved sst "$sst_before"
echo 'SST probe cancellation checked'

# An interrupted repair must retain the old SST until its replacement is ready.
# Force its classification as corrupt without modifying the object in S3.
query --query "DETACH TABLE ${database}.sst SYNC; SYSTEM ENABLE FAILPOINT unique_key_sst_force_corruption"
cancel_at "ATTACH TABLE ${database}.sst" unique_key_before_index_rebuild timeout
query --query "ATTACH TABLE IF NOT EXISTS ${database}.sst; SYSTEM DISABLE FAILPOINT unique_key_sst_force_corruption"
assert_preserved sst "$sst_before"
echo 'SST repair cancellation checked'

# A legacy part without an SST entry is rebuilt from its intact columns on attach.
# `Ordinary` permits redefining a detached table through SQL without editing its metadata.
query --allow_deprecated_database_ordinary=1 --query "
    CREATE DATABASE ${legacy_database} ENGINE=Ordinary SETTINGS disk='default';
    CREATE TABLE ${legacy_database}.rebuild (n UInt64) ENGINE=MergeTree ORDER BY n SETTINGS ${part_settings};
    SYSTEM STOP MERGES ${legacy_database}.rebuild;
    INSERT INTO ${legacy_database}.rebuild SELECT number FROM numbers(4096) SETTINGS max_insert_threads=1, max_block_size=65536, min_insert_block_size_rows=65536, min_insert_block_size_bytes=0;
"
rebuild_before=$(state rebuild "$legacy_database")
rebuild_attach="ATTACH TABLE ${legacy_database}.rebuild (n UInt64)
    ENGINE=MergeTree UNIQUE KEY (n) ORDER BY n SETTINGS ${part_settings}"
query --query "DETACH TABLE ${legacy_database}.rebuild SYNC"
cancel_at "$rebuild_attach" unique_key_before_index_rebuild kill
query --query "$rebuild_attach"
assert_preserved rebuild "$rebuild_before" "$legacy_database"
echo 'SST rebuild cancellation checked'
