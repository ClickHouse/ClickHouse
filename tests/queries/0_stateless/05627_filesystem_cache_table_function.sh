#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-object-storage, no-random-settings
# no-fasttest: needs S3.
# no-parallel: uses the caches of `02944_dynamically_change_filesystem_cache_size`, which changes their config.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

lru="s3_cache_02944_lru"
slru="s3_cache_02944_slru"

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS test_lru;
CREATE TABLE test_lru (a String) ENGINE = MergeTree ORDER BY tuple() SETTINGS disk = '$lru', serialization_info_version = 'basic';
INSERT INTO test_lru SELECT randomString(100);
SYSTEM CLEAR FILESYSTEM CACHE '$lru';
SELECT * FROM test_lru FORMAT Null;
"

echo "LRU: the same segments as system.filesystem_cache"
$CLICKHOUSE_CLIENT -m --query "
SELECT count(), groupUniqArray(state), groupUniqArray(queue) FROM filesystemCache('$lru', 100, 1000000);
SELECT priority_rank, state, queue FROM filesystemCache('$lru', 100, 1000000);
SELECT count() FROM
(
    SELECT path, key, offset, size, state FROM filesystemCache('$lru', 100, 1000000)
    EXCEPT
    SELECT cache_path, key, file_segment_range_begin, size, state FROM system.filesystem_cache WHERE cache_name = '$lru'
);
SELECT count() FROM
(
    SELECT cache_path, key, file_segment_range_begin, size, state FROM system.filesystem_cache WHERE cache_name = '$lru'
    EXCEPT
    SELECT path, key, offset, size, state FROM filesystemCache('$lru', 100, 1000000)
);
"

echo "LRU: ranks and order"
$CLICKHOUSE_CLIENT -m --query "
SELECT groupArray(priority_rank) = range(1, count() + 1) FROM filesystemCache('$lru', 100, 1000000, 'hottest');
SELECT groupArray(priority_rank) = range(1, count() + 1) FROM filesystemCache('$lru', 100, 1000000, 'coldest');
SELECT
    (SELECT arrayMap(x -> x.2, arraySort(groupArray((priority_rank, path)))) FROM filesystemCache('$lru', 100, 1000000, 'hottest'))
    = arrayReverse((SELECT arrayMap(x -> x.2, arraySort(groupArray((priority_rank, path)))) FROM filesystemCache('$lru', 100, 1000000, 'coldest')));
-- 'hottest' is the default.
SELECT
    (SELECT groupArray((priority_rank, path)) FROM filesystemCache('$lru', 100, 1000000))
    = (SELECT groupArray((priority_rank, path)) FROM filesystemCache('$lru', 100, 1000000, 'hottest'));
"

echo "LRU: max_elements"
$CLICKHOUSE_CLIENT -m --query "
SELECT count() FROM filesystemCache('$lru', 3, 1000000, 'coldest');
SELECT
    (SELECT arraySort(groupArray((priority_rank, path))) FROM filesystemCache('$lru', 3, 1000000, 'coldest'))
    = (SELECT arraySlice(arraySort(groupArray((priority_rank, path))), 1, 3) FROM filesystemCache('$lru', 100, 1000000, 'coldest'));
SELECT count() FROM filesystemCache('$lru', 0, 1000000, 'coldest');
"

echo "LRU: max_total_size"
# The walk stops at the first segment which does not fit, so the result is the longest prefix of the full walk within the limit.
for order in hottest coldest; do
    $CLICKHOUSE_CLIENT --query "
    WITH (SELECT arraySort(groupArray((priority_rank, path, size))) FROM filesystemCache('$lru', 100, 1000000, '$order')) AS full_walk
    SELECT
        count() > 0,
        arraySort(groupArray((priority_rank, path, size)))
            = arraySlice(full_walk, 1, length(arrayFilter(s -> s <= 25, arrayCumSum(arrayMap(x -> x.3, full_walk))))),
        sum(size) <= 25
    FROM filesystemCache('$lru', 100, 25, '$order');
    "
done
$CLICKHOUSE_CLIENT --query "SELECT count() FROM filesystemCache('$lru', 100, 0)"

echo "LRU: random"
$CLICKHOUSE_CLIENT -m --query "
-- A sample of everything is the whole cache, with the ranks of 'coldest'.
SELECT
    (SELECT arraySort(groupArray((priority_rank, path))) FROM filesystemCache('$lru', 100, 1000000, 'random'))
    = (SELECT arraySort(groupArray((priority_rank, path))) FROM filesystemCache('$lru', 100, 1000000, 'coldest'));
-- A smaller sample: distinct ranks, and every row is the segment with that rank in 'coldest'.
SELECT count(), uniqExact(priority_rank) = count() FROM filesystemCache('$lru', 3, 1000000, 'random');
SELECT count(), countIf(r.path != c.path)
FROM filesystemCache('$lru', 3, 1000000, 'random') AS r
INNER JOIN filesystemCache('$lru', 100, 1000000, 'coldest') AS c USING (priority_rank);
SELECT sum(size) <= 25 FROM filesystemCache('$lru', 100, 25, 'random');
"

echo "SLRU: the probationary queue is colder than the protected one"
s3_prefix="http://localhost:11111/test/${CLICKHOUSE_DATABASE}/05627"
$CLICKHOUSE_CLIENT -m --query "
INSERT INTO FUNCTION s3('${s3_prefix}_a.txt', 'clickhouse', 'clickhouse', 'RawBLOB') SETTINGS s3_truncate_on_insert = 1 SELECT 'abcdefgh';
INSERT INTO FUNCTION s3('${s3_prefix}_b.txt', 'clickhouse', 'clickhouse', 'RawBLOB') SETTINGS s3_truncate_on_insert = 1 SELECT 'ijklmnop';
SYSTEM CLEAR FILESYSTEM CACHE '$slru';
SELECT * FROM s3('${s3_prefix}_a.txt', 'clickhouse', 'clickhouse', 'RawBLOB') FORMAT Null SETTINGS filesystem_cache_name = '$slru';
SELECT * FROM s3('${s3_prefix}_a.txt', 'clickhouse', 'clickhouse', 'RawBLOB') FORMAT Null SETTINGS filesystem_cache_name = '$slru';
SELECT * FROM s3('${s3_prefix}_b.txt', 'clickhouse', 'clickhouse', 'RawBLOB') FORMAT Null SETTINGS filesystem_cache_name = '$slru';
SELECT 'coldest';
SELECT priority_rank, queue, offset, size FROM filesystemCache('$slru', 100, 1000000, 'coldest');
SELECT 'hottest';
SELECT priority_rank, queue, offset, size FROM filesystemCache('$slru', 100, 1000000, 'hottest');
"

echo "SplitCache: the data queue is always walked before the system one"
split="cache_05627_split"
# Attaching the table reads its metadata files (System), the select reads its data (Data).
$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS test_split;
CREATE TABLE test_split (a String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS disk = disk(type = cache, name = '$split', path = '$split', disk = 's3_disk_02944', use_split_cache = 1,
    max_size = '1Mi', max_elements = 1000, max_file_segment_size = '16Ki', boundary_alignment = '16Ki'),
    serialization_info_version = 'basic';
INSERT INTO test_split SELECT randomString(100);
SYSTEM CLEAR FILESYSTEM CACHE '$split';
DETACH TABLE test_split;
ATTACH TABLE test_split;
SELECT * FROM test_split FORMAT Null;
"
$CLICKHOUSE_CLIENT -m --query "
SELECT arraySort(groupUniqArray(queue)) = ['SplitCache_Data', 'SplitCache_System'] FROM filesystemCache('$split', 1000, 1000000);
SELECT count() FROM
(
    SELECT path, queue FROM filesystemCache('$split', 1000, 1000000)
    EXCEPT
    SELECT cache_path, concat('SplitCache_', segment_type) FROM system.filesystem_cache WHERE cache_name = '$split'
);
"
for order in hottest coldest random; do
    $CLICKHOUSE_CLIENT --query "
    SELECT '$order', maxIf(priority_rank, queue = 'SplitCache_Data') < minIf(priority_rank, queue = 'SplitCache_System')
    FROM filesystemCache('$split', 1000, 1000000, '$order')
    HAVING countIf(queue = 'SplitCache_Data') > 0 AND countIf(queue = 'SplitCache_System') > 0;
    "
done
# When the limit is reached in the data queue, the system queue is not walked.
data_count=$($CLICKHOUSE_CLIENT --query "SELECT countIf(queue = 'SplitCache_Data') FROM filesystemCache('$split', 1000, 1000000)")
$CLICKHOUSE_CLIENT --query "SELECT groupUniqArray(queue) FROM filesystemCache('$split', $data_count, 1000000, 'coldest')"

echo "Access: requires SELECT on system.filesystem_cache"
user="user_05627_${CLICKHOUSE_DATABASE}"
$CLICKHOUSE_CLIENT -m --query "
DROP USER IF EXISTS $user;
CREATE USER $user IDENTIFIED WITH no_password;
"
$CLICKHOUSE_CLIENT --user "$user" --query "SELECT count() FROM filesystemCache('$lru', 100, 1000000)" 2>&1 | grep -o -m1 'ACCESS_DENIED'
# A grant on some columns only is not enough.
$CLICKHOUSE_CLIENT --query "GRANT SELECT(path) ON system.filesystem_cache TO $user"
$CLICKHOUSE_CLIENT --user "$user" --query "SELECT count() FROM filesystemCache('$lru', 100, 1000000)" 2>&1 | grep -o -m1 'ACCESS_DENIED'
$CLICKHOUSE_CLIENT --query "GRANT SELECT ON system.filesystem_cache TO $user"
$CLICKHOUSE_CLIENT --user "$user" --query "SELECT count() > 0 FROM filesystemCache('$lru', 100, 1000000)"
$CLICKHOUSE_CLIENT --query "DROP USER $user"

echo "Errors"
$CLICKHOUSE_CLIENT -m --query "
SELECT * FROM filesystemCache('$lru', 100); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM filesystemCache('$lru', 100, 1000, 'hottest', 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT * FROM filesystemCache('$lru', 100, 1000, 'warmest'); -- { serverError BAD_ARGUMENTS }
SELECT * FROM filesystemCache('no_such_cache_05627', 100, 1000); -- { serverError BAD_ARGUMENTS }
"

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE test_lru;
DROP TABLE test_split;
"
