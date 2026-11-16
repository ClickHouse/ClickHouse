-- Tags: no-fasttest
-- no-fasttest: requires a cache disk.

-- `efficiency_window_sec` is at most one day.
CREATE TABLE t_05619 (key UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS disk = disk(type = cache, name = 'cache_05619_one_day', path = 'cache_05619_one_day', disk = 'local_disk',
    max_size = '16Mi', max_file_segment_size = '4Mi', efficiency_window_sec = 86400);
DROP TABLE t_05619;

CREATE TABLE t_05619 (key UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS disk = disk(type = cache, name = 'cache_05619_longer', path = 'cache_05619_longer', disk = 'local_disk',
    max_size = '16Mi', max_file_segment_size = '4Mi', efficiency_window_sec = 86401); -- { serverError BAD_ARGUMENTS }

-- In milliseconds, 2^61 seconds overflow to 0.
CREATE TABLE t_05619 (key UInt64) ENGINE = MergeTree ORDER BY key
SETTINGS disk = disk(type = cache, name = 'cache_05619_overflow', path = 'cache_05619_overflow', disk = 'local_disk',
    max_size = '16Mi', max_file_segment_size = '4Mi', efficiency_window_sec = 2305843009213693952); -- { serverError BAD_ARGUMENTS }

SELECT cache_name, efficiency_window_sec
FROM system.filesystem_cache_settings
WHERE cache_name LIKE 'cache_05619_%'
ORDER BY cache_name;
