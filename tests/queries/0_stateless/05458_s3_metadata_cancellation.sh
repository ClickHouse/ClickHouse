#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-shared-merge-tree
# no-fasttest: requires S3 and Keeper.
# no-parallel: pauses server-wide checking failpoints and injects a cancellation exception.
# no-shared-merge-tree: exercises the local and replicated part-checking paths.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

database="${CLICKHOUSE_DATABASE}_metadata_cancellation"
# shellcheck source=./s3_cancellation.lib
. "$CUR_DIR"/s3_cancellation.lib

failpoints=(check_data_part_before_serialization_read check_data_part_before_projection_read
            query_status_cancel_with_injected_exception load_data_part_before_metadata_read
            load_projection_before_metadata_read)

cleanup()
{
    disable_failpoints >/dev/null 2>&1 ||:
    if [[ -n "$client_pid" ]]; then
        query --query "KILL QUERY WHERE query_id='${query_id}' ASYNC FORMAT Null" >/dev/null 2>&1 ||:
        wait "$client_pid" ||:
    fi
    query --query "ATTACH TABLE IF NOT EXISTS ${database}.plain" >/dev/null 2>&1 ||:
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
    [[ $(query --query "SELECT count(), sum(n) FROM ${database}.${table};
        SELECT count() > 0 AND countIf(is_broken OR exception_code != 0) = 0
        FROM system.projection_parts WHERE database='${database}' AND table='${table}' AND active") == $'4096\t8386560\n1' ]]
done
query --query "SYSTEM STOP FETCHES ${database}.replicated"
declare -A initial_state
for table in plain replicated; do
    initial_state[$table]=$(state "$table")
done

cancel_check()
{
    local table=$1 target=$2 method=$3 single_value=$4
    local failpoint=check_data_part_before_serialization_read gate=""
    if [[ "$target" == projection ]]; then
        failpoint=check_data_part_before_projection_read
    elif [[ "$target" == projection_serialization ]]; then
        gate=check_data_part_before_projection_read
    fi
    cancel_at "CHECK TABLE ${database}.${table} SETTINGS check_query_single_value_result=${single_value}" \
        "$failpoint" "$method" "$gate"
    assert_preserved "$table" "${initial_state[$table]}"
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
for failpoint in load_data_part_before_metadata_read load_projection_before_metadata_read; do
    query --query "DETACH TABLE ${database}.plain SYNC; SYSTEM DROP PAGE CACHE"
    cancel_at "ATTACH TABLE ${database}.plain" "$failpoint" kill
    query --query "ATTACH TABLE ${database}.plain"
    assert_preserved plain "${initial_state[plain]}"
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
[[ $(query --query "SELECT count(), sum(n) FROM ${database}.cached SETTINGS enable_filesystem_cache=1;
    CHECK TABLE ${database}.cached SETTINGS check_query_single_value_result=1") == $'4096\t8386560\n1' ]]
echo 'cache and data preserved'
