#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-parallel
# no-parallel: the cache directory of the test is fixed, so concurrent runs of the test would share it.
# no-replicated-database: the check scans the descriptors of the local server.

# The objects of a `borrow_from_cache` disk are `Ephemeral` file segments, which stay held for as
# long as the object exists. Writing an object must not leave the file of the segment open for
# writing until then, otherwise every stored object keeps a file descriptor.
#
# The check looks for descriptors open for writing into the files of the cache, as
# `04403_mergetree_close_fds_before_part_rename` does for parts. The server-wide `OpenFileForWrite`
# metric is not usable: under load, the flushes of system logs keep thousands of files open for
# writing for seconds.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS cache_creator SYNC;
CREATE TABLE cache_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '05448_cache_creator',
    path = '05448_borrow_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

DROP TABLE IF EXISTS borrowed SYNC;
CREATE TABLE borrowed (k UInt64)
ENGINE = MergeTree() ORDER BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '05448_cache_creator',
    name = '05448_borrowed_disk'
);
SYSTEM STOP MERGES borrowed;
"

# 100 parts of several files each.
$CLICKHOUSE_CLIENT -q "
INSERT INTO borrowed SELECT number FROM numbers(100)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0, max_insert_threads = 1;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'borrowed' AND active;
SELECT count(), sum(k) FROM borrowed;
"

# The files of the objects. Check that they are visible from here, otherwise the scan below would
# pass vacuously.
cache_files=$($CLICKHOUSE_CLIENT -q "SELECT cache_path FROM system.filesystem_cache WHERE cache_name = '05448_cache_creator' AND kind = 'Ephemeral'")
visible=0
for file in $cache_files
do
    [ -f "$file" ] && visible=$((visible + 1))
done
[ "$visible" -gt 100 ] && echo "cache files are visible" || echo "only $visible cache files are visible"
cache_dir=$(dirname "$(dirname "$(dirname "$(echo "$cache_files" | head -n1)")")")

# Descriptors (of any process) that point into the files of segments and are open for writing. The
# segments are in the subdirectories of the cache, while the cache itself keeps its `status` file in
# the top directory open. The access mode is in the low two bits of the octal `flags` of
# `/proc/PID/fdinfo/FD` (0 is `O_RDONLY`).
function write_descriptors_into_cache()
{
    local count=0
    local fd_link flags
    while read -r fd_link
    do
        flags=$(grep -m1 '^flags:' "${fd_link/\/fd\//\/fdinfo\/}" 2>/dev/null | tr -dc '0-7')
        [ -n "$flags" ] || continue
        [ $(( 8#$flags & 3 )) -ne 0 ] && count=$((count + 1))
    done < <(find /proc/[0-9]*/fd -mindepth 1 -maxdepth 1 -lname "${cache_dir}/*/*" 2>/dev/null)
    echo "$count"
}

# The cache keeps its `status` file open, so if the descriptors of the server cannot be seen from here,
# the scan would pass vacuously.
find /proc/[0-9]*/fd -mindepth 1 -maxdepth 1 -lname "${cache_dir}/status" 2>/dev/null | grep -q . \
    && echo "descriptors of the server are visible" || echo "descriptors of the server are not visible"

# A leak keeps about a thousand descriptors open for as long as the table exists. A descriptor that
# is open only for a moment (a concurrent write into the cache) does not persist, so look several
# times and take the smallest count.
min=""
for _ in {1..5}
do
    count=$(write_descriptors_into_cache)
    if [ -z "$min" ] || [ "$count" -lt "$min" ]
    then
        min=$count
    fi
    sleep 0.1
done
echo "write descriptors into the cache: $min"

$CLICKHOUSE_CLIENT -q "
DROP TABLE borrowed SYNC;
DROP TABLE cache_creator SYNC;
"
