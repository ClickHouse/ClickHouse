#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-shared-merge-tree
# no-fasttest: requires S3, Keeper, RocksDB and USearch.
# no-parallel: pauses server-wide checking failpoints and injects a cancellation exception.
# no-shared-merge-tree: exercises the local and replicated part-checking paths.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

database="${CLICKHOUSE_DATABASE}_metadata_cancellation"
query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_check"
output="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.out"
error="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"
client_pid=""
cancel_number=0
read_options=()
failpoints=(check_data_part_before_serialization_read check_data_part_before_projection_read
            query_status_cancel_with_injected_exception load_data_part_before_metadata_read
            load_projection_before_metadata_read check_consistency_before_marks_read
            vector_similarity_index_before_read unique_key_sst_before_read unique_key_before_index_rebuild)

query()
{
    # shellcheck disable=SC2086
    timeout 120 $CLICKHOUSE_CLIENT --allow_repeated_settings --async_insert=0 --enable_unique_key=1 --send_logs_level=none \
        --enable_filesystem_cache=0 --enable_filesystem_cache_on_write_operations=0 \
        --use_page_cache_for_object_storage=0 --use_page_cache_for_disks_without_file_cache=0 "${read_options[@]}" "$@"
}

cleanup()
{
    for failpoint in "${failpoints[@]}"; do
        query --query "SYSTEM DISABLE FAILPOINT ${failpoint}" >/dev/null 2>&1 ||:
    done
    if [[ -n "$client_pid" ]]; then
        query --query "KILL QUERY WHERE query_id='${query_id}' ASYNC FORMAT Null" >/dev/null 2>&1 ||:
        wait "$client_pid" ||:
    fi
    for table in plain consistency sst rebuild; do
        query --query "ATTACH TABLE IF NOT EXISTS ${database}.${table}" >/dev/null 2>&1 ||:
    done
    query --query "DROP DATABASE IF EXISTS ${database} SYNC" >/dev/null 2>&1 ||:
}
trap cleanup EXIT

query --query "CREATE DATABASE ${database} ENGINE=Atomic SETTINGS disk='default'"
for table in plain replicated; do
    engine=MergeTree
    if [[ "$table" == replicated ]]; then
        engine="ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/check_cancellation', 'r1')"
    fi
    query --query "
        CREATE TABLE ${database}.${table} (n UInt64, PROJECTION p (SELECT n ORDER BY n))
        ENGINE=${engine} ORDER BY n
        SETTINGS storage_policy='s3_no_cache', min_bytes_for_wide_part=0,
                 ratio_of_defaults_for_sparse_serialization=0.5,
                 prewarm_mark_cache=0, prewarm_primary_key_cache=0;
        SYSTEM STOP MERGES ${database}.${table};
        INSERT INTO ${database}.${table} SELECT number FROM numbers(4096);
    "
    [[ $(query --query "SELECT count(), sum(n) FROM ${database}.${table}") == $'4096\t8386560' ]]
    [[ $(query --query "SELECT count() > 0 AND countIf(is_broken OR exception_code != 0) = 0
        FROM system.projection_parts WHERE database='${database}' AND table='${table}' AND active") == 1 ]]
done
query --query "SYSTEM STOP FETCHES ${database}.replicated"

state()
{
    query --query "SELECT
        (SELECT (count(), sum(n)) FROM ${database}.$1),
        (SELECT arraySort(groupArray(name)) FROM system.parts
         WHERE database='${database}' AND table='$1' AND active),
        (SELECT arraySort(groupArray((name, is_broken, exception_code, exception))) FROM system.projection_parts
         WHERE database='${database}' AND table='$1' AND active),
        (SELECT arraySort(groupArray((name, reason))) FROM system.detached_parts
         WHERE database='${database}' AND table='$1'),
        (SELECT count() FROM system.replication_queue
         WHERE database='${database}' AND table='$1' AND type='GET_PART')"
}

cancel_at()
{
    local statement=$1 failpoint=$2 method=$3 gate=${4:-}
    query_id="${CLICKHOUSE_TEST_UNIQUE_NAME}_check_$((++cancel_number))"
    local expected_error=QUERY_WAS_CANCELLED timeout_seconds=0 result=0
    if [[ "$method" == timeout ]]; then
        expected_error=TIMEOUT_EXCEEDED
        timeout_seconds=5
    elif [[ "$method" == injected ]]; then
        expected_error=FAULT_INJECTED
    fi
    query --query "SYSTEM ENABLE FAILPOINT ${gate:-$failpoint}"
    if [[ "$method" == injected ]]; then
        query --query "SYSTEM ENABLE FAILPOINT query_status_cancel_with_injected_exception"
    fi
    query --query_id "$query_id" --max_threads=1 --max_execution_time="$timeout_seconds" \
        --timeout_overflow_mode=throw --query "$statement" >"$output" 2>"$error" &
    client_pid=$!
    if [[ -n "$gate" ]]; then
        query --query "SYSTEM WAIT FAILPOINT ${gate} PAUSE"
        # Arm the nested serialization pause only after checking the parent part.
        query --query "SYSTEM ENABLE FAILPOINT ${failpoint}; SYSTEM DISABLE FAILPOINT ${gate}"
    fi
    query --query "SYSTEM WAIT FAILPOINT ${failpoint} PAUSE"
    if [[ "$method" != timeout ]]; then
        query --query "KILL QUERY WHERE query_id='${query_id}' ASYNC FORMAT Null"
    fi
    local deadline=$((SECONDS + 60))
    while [[ $(query --query "SELECT count() FROM system.processes WHERE query_id='${query_id}' AND is_cancelled") != 1 ]]; do
        (( SECONDS < deadline )) || return 1
        sleep 0.1
    done
    query --query "SYSTEM DISABLE FAILPOINT ${failpoint}"
    wait "$client_pid" || result=$?
    client_pid=""
    query --query "SYSTEM DISABLE FAILPOINT query_status_cancel_with_injected_exception"
    local actual_code
    actual_code=$(sed -n 's/^Code: \([0-9]*\)\..*/\1/p' "$error" | head -1)
    if (( result == 0 )) || [[ ! "$actual_code" =~ ^[0-9]+$ ]] \
        || [[ $(query --query "SELECT errorCodeToName(${actual_code})") != "$expected_error" || -s "$output" ]]; then
        cat "$output" "$error"
        echo "Expected ${expected_error} with no result rows, client exit status: ${result}"
        return 1
    fi
    if [[ "$method" == injected ]]; then
        grep -q 'Injected query cancellation exception' "$error"
    fi
}

assert_preserved()
{
    local table=$1 before=$2 after
    after=$(state "$table")
    if [[ "$after" != "$before" ]]; then
        diff -u <(printf '%s\n' "$before") <(printf '%s\n' "$after")
        return 1
    fi
    [[ $(query --query "CHECK TABLE ${database}.${table} SETTINGS check_query_single_value_result=1") == 1 ]]
}

cancel_check()
{
    local table=$1 target=$2 method=$3 single_value=$4
    local failpoint=check_data_part_before_serialization_read gate="" before
    if [[ "$target" == projection ]]; then
        failpoint=check_data_part_before_projection_read
    elif [[ "$target" == projection_serialization ]]; then
        gate=check_data_part_before_projection_read
    fi
    before=$(state "$table")
    cancel_at "CHECK TABLE ${database}.${table} SETTINGS check_query_single_value_result=${single_value}" \
        "$failpoint" "$method" "$gate"
    assert_preserved "$table" "$before"
    echo "${table} ${target} ${method} ${single_value}: preserved"
}

# Cover both storage handlers and result formats without a Cartesian matrix.
cancel_check plain part kill 0
cancel_check plain projection timeout 1
cancel_check replicated part timeout 0
cancel_check replicated projection kill 1
cancel_check replicated projection injected 0
cancel_check replicated projection_serialization timeout 1

# Both metadata loaders must leave a cancelled attach recoverable.
plain_before=$(state plain)
for failpoint in load_data_part_before_metadata_read load_projection_before_metadata_read; do
    query --query "DETACH TABLE ${database}.plain SYNC; SYSTEM DROP PAGE CACHE"
    cancel_at "ATTACH TABLE ${database}.plain" "$failpoint" kill
    query --query "ATTACH TABLE ${database}.plain"
    assert_preserved plain "$plain_before"
    echo "${failpoint}: preserved"
done

# Use a dedicated cache so unrelated background reads cannot change its contents.
query --query "
    CREATE TABLE ${database}.cached (n UInt64) ENGINE=MergeTree ORDER BY n
    SETTINGS storage_policy='s3_cache_cancellation', min_bytes_for_wide_part=0,
             ratio_of_defaults_for_sparse_serialization=0.5,
             prewarm_mark_cache=0, prewarm_primary_key_cache=0;
    SYSTEM STOP MERGES ${database}.cached;
    INSERT INTO ${database}.cached SELECT number FROM numbers(4096);
    SYSTEM DROP FILESYSTEM CACHE 's3_cache_cancellation';
"
[[ $(query --enable_filesystem_cache=1 --read_from_filesystem_cache_if_exists_otherwise_bypass_cache=0 \
    --query "SELECT sum(n) FROM ${database}.cached") == 8386560 ]]
cache_state()
{
    query --query "SELECT key, file_segment_range_begin, size, state FROM system.filesystem_cache
        WHERE cache_name='s3_cache_cancellation' ORDER BY key, file_segment_range_begin, size, state"
}
cache_before=$(cache_state)
[[ -n "$cache_before" ]]
# Cancellation must not enter the corrupt-data recovery path that evicts cached files.
cancel_at "CHECK TABLE ${database}.cached SETTINGS check_query_single_value_result=0" \
    check_data_part_before_serialization_read kill
cache_after=$(cache_state)
# `CHECK TABLE` can cache metadata before reaching the pause. Previously cached
# segments must survive unchanged, but additional segments are legitimate.
missing_segments=$(LC_ALL=C comm -23 \
    <(printf '%s\n' "$cache_before" | LC_ALL=C sort) \
    <(printf '%s\n' "$cache_after" | LC_ALL=C sort))
if [[ -n "$missing_segments" ]]; then
    echo 'Cached segments disappeared or changed:'
    printf '%s\n' "$missing_segments"
    exit 1
fi
[[ $(query --enable_filesystem_cache=1 --query "SELECT count(), sum(n) FROM ${database}.cached") == $'4096\t8386560' ]]
[[ $(query --query "CHECK TABLE ${database}.cached SETTINGS check_query_single_value_result=1") == 1 ]]
echo 'cache and data preserved'

# Exercise cancellation inside the index readers, before any asynchronous prefetch.
read_options=(--max_read_buffer_size=4096 --max_read_buffer_size_remote_fs=4096
              --remote_filesystem_read_method=read --remote_filesystem_read_prefetch=0
              --allow_prefetched_read_pool_for_remote_filesystem=0 --use_uncompressed_cache=0
              --max_parallel_replicas=1 --enable_parallel_replicas=0)
part_settings="storage_policy='s3_no_cache', min_bytes_for_wide_part=0, min_rows_for_wide_part=0,
    min_bytes_for_full_part_storage=0, min_rows_for_full_part_storage=0, min_level_for_full_part_storage=0,
    prewarm_mark_cache=0, prewarm_primary_key_cache=0"

# Regenerating checksums runs the marks check inside the diagnostic exception handler.
query --query "
    CREATE TABLE ${database}.consistency (n UInt64) ENGINE=MergeTree ORDER BY n SETTINGS ${part_settings};
    SYSTEM STOP MERGES ${database}.consistency;
    INSERT INTO ${database}.consistency SELECT number FROM numbers(4096) SETTINGS max_insert_threads=1, max_block_size=65536, min_insert_block_size_rows=65536, min_insert_block_size_bytes=0;
"
consistency_before=$(state consistency)
part_path=$(query --query "SELECT path FROM system.parts WHERE database='${database}' AND table='consistency' AND active")
[[ "$part_path" == /* && "$part_path" != *$'\n'* ]]
query --query "DETACH TABLE ${database}.consistency SYNC; SYSTEM DROP MARK CACHE"
rm "${part_path}checksums.txt"
cancel_at "ATTACH TABLE ${database}.consistency" check_consistency_before_marks_read timeout
query --query "ATTACH TABLE IF NOT EXISTS ${database}.consistency"
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
sst_key=$(query --query "SELECT remote_path FROM system.remote_data_paths
    WHERE disk_name='s3_no_cache' AND local_path=(SELECT concat(substring(path, position(path, 'store/')), 'unique_key_index.sst')
        FROM system.parts WHERE database='${database}' AND table='sst' AND active)")
[[ -n "$sst_key" && "$sst_key" != *$'\n'* ]]
sst_file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.sst"
curl --fail --silent --show-error "http://localhost:11111/test/${sst_key}" -o "$sst_file"
python3 - "$sst_file" <<'PY'
import sys
from pathlib import Path
path = Path(sys.argv[1])
data = bytearray(path.read_bytes())
data[len(data) // 2] ^= 255
path.write_bytes(data)
PY
query --query "DETACH TABLE ${database}.sst SYNC"
curl --fail --silent --show-error -X PUT "http://localhost:11111/test/${sst_key}" --upload-file "$sst_file"
cancel_at "ATTACH TABLE ${database}.sst" unique_key_before_index_rebuild timeout
query --query "ATTACH TABLE IF NOT EXISTS ${database}.sst"
assert_preserved sst "$sst_before"
echo 'SST repair cancellation checked'

# A legacy part without an SST entry is rebuilt from its intact columns on attach.
query --query "
    CREATE TABLE ${database}.rebuild (n UInt64) ENGINE=MergeTree ORDER BY n SETTINGS ${part_settings};
    SYSTEM STOP MERGES ${database}.rebuild;
    INSERT INTO ${database}.rebuild SELECT number FROM numbers(4096) SETTINGS max_insert_threads=1, max_block_size=65536, min_insert_block_size_rows=65536, min_insert_block_size_bytes=0;
"
rebuild_before=$(state rebuild)
metadata_path=$(query --query "SELECT metadata_path FROM system.tables WHERE database='${database}' AND name='rebuild'")
if [[ "$metadata_path" != /* ]]; then
    metadata_path="$(query --query "SELECT path FROM system.disks WHERE name='default'")${metadata_path}"
fi
query --query "DETACH TABLE ${database}.rebuild SYNC"
sed -i 's/^ORDER BY/UNIQUE KEY (n)\nORDER BY/' "$metadata_path"
cancel_at "ATTACH TABLE ${database}.rebuild" unique_key_before_index_rebuild kill
query --query "ATTACH TABLE IF NOT EXISTS ${database}.rebuild"
assert_preserved rebuild "$rebuild_before"
echo 'SST rebuild cancellation checked'
