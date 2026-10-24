-- Tags: no-parallel-replicas
-- no-parallel-replicas: the query condition cache is populated per replica

-- The query condition cache serves a condition whose time bounds changed since it was cached (issues #78502, #115504):
-- a granule recorded as not matching an earlier variant of the condition is skipped unless the primary key shows
-- that it may have rows admitted by the new bounds but not by the earlier ones.

-- The cache records granules only from reads that are not split into layers. Granules are computed from
-- `read_rows`, so their size has to be fixed. Small gaps between ranges must not be read.
SET use_query_condition_cache = 1, enable_analyzer = 1,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0;

-- One row per millisecond from 2025-01-01 00:00:00 UTC (1735689600000 ms), 123 granules, a single part.
-- The only matching rows are in the granule of the row at 500 seconds.
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (time DateTime64(3, 'UTC'), num UInt64) ENGINE = MergeTree ORDER BY time
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
INSERT INTO tab SELECT fromUnixTimestamp64Milli(toInt64(1735689600000 + number), 'UTC'), number IN (500000, 500001)
FROM numbers(1000000)
SETTINGS max_insert_threads = 1, max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

-- Prime the cache: the bounds select 98 granules, which are all read.
SELECT 'where prime', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689700000, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690500000, 'UTC') AND num = 1 SETTINGS optimize_move_to_prewhere = 0;
-- Both bounds moved by 3 ms: only the granule with matches and the granule at the new upper bound are read.
SELECT 'where shifted', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689700003, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690500003, 'UTC') AND num = 1 SETTINGS optimize_move_to_prewhere = 0;
-- Narrower bounds: only the granule with matches.
SELECT 'where narrower', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689750000, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690450000, 'UTC') AND num = 1 SETTINGS optimize_move_to_prewhere = 0;
-- Wider bounds: also the granules between the earlier and the new bounds.
SELECT 'where wider', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689680000, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690520000, 'UTC') AND num = 1 SETTINGS optimize_move_to_prewhere = 0;
-- The same bounds as a string, compared with the `DateTime64` column.
SELECT 'where string', count() FROM tab WHERE time >= '2025-01-01 00:01:20.000' AND time <= '2025-01-01 00:15:20.000' AND num = 1 SETTINGS optimize_move_to_prewhere = 0;
SELECT 'where string shifted', count() FROM tab WHERE time >= '2025-01-01 00:01:20.005' AND time <= '2025-01-01 00:15:20.005' AND num = 1 SETTINGS optimize_move_to_prewhere = 0;
-- Under NOT, the direction of the bounds flips.
SELECT 'where not', count() FROM tab WHERE NOT (time < fromUnixTimestamp64Milli(1735689700000, 'UTC') OR time > fromUnixTimestamp64Milli(1735690500000, 'UTC') OR num != 1) SETTINGS optimize_move_to_prewhere = 0;
SELECT 'where not shifted', count() FROM tab WHERE NOT (time < fromUnixTimestamp64Milli(1735689700003, 'UTC') OR time > fromUnixTimestamp64Milli(1735690500003, 'UTC') OR num != 1) SETTINGS optimize_move_to_prewhere = 0;
-- A granule recorded as not matching an earlier variant is read if it has rows in the band: the earlier
-- bounds exclude the matching rows, the new ones include them.
SELECT 'where before matches', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689700000, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690000000, 'UTC') AND num = 1 SETTINGS optimize_move_to_prewhere = 0;
SELECT 'where including matches', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689700000, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690200000, 'UTC') AND num = 1 SETTINGS optimize_move_to_prewhere = 0;
-- Other conditions do not share entries.
SELECT 'where other condition', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689700003, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690500003, 'UTC') AND num = 0 SETTINGS optimize_move_to_prewhere = 0;

TRUNCATE TABLE tab;
INSERT INTO tab SELECT fromUnixTimestamp64Milli(toInt64(1735689600000 + number), 'UTC'), number IN (500000, 500001)
FROM numbers(1000000)
SETTINGS max_insert_threads = 1, max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

-- Prime the cache: the bounds select 98 granules, which are all read.
SELECT 'prewhere prime', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689700000, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690500000, 'UTC') AND num = 1 SETTINGS optimize_move_to_prewhere = 1;
-- Both bounds moved by 3 ms: only the granule with matches and the granule at the new upper bound are read.
SELECT 'prewhere shifted', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689700003, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690500003, 'UTC') AND num = 1 SETTINGS optimize_move_to_prewhere = 1;
-- Narrower bounds: only the granule with matches.
SELECT 'prewhere narrower', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689750000, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690450000, 'UTC') AND num = 1 SETTINGS optimize_move_to_prewhere = 1;
-- Wider bounds: also the granules between the earlier and the new bounds.
SELECT 'prewhere wider', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689680000, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690520000, 'UTC') AND num = 1 SETTINGS optimize_move_to_prewhere = 1;
-- The same bounds as a string, compared with the `DateTime64` column.
SELECT 'prewhere string', count() FROM tab WHERE time >= '2025-01-01 00:01:20.000' AND time <= '2025-01-01 00:15:20.000' AND num = 1 SETTINGS optimize_move_to_prewhere = 1;
SELECT 'prewhere string shifted', count() FROM tab WHERE time >= '2025-01-01 00:01:20.005' AND time <= '2025-01-01 00:15:20.005' AND num = 1 SETTINGS optimize_move_to_prewhere = 1;
-- Under NOT, the direction of the bounds flips.
SELECT 'prewhere not', count() FROM tab WHERE NOT (time < fromUnixTimestamp64Milli(1735689700000, 'UTC') OR time > fromUnixTimestamp64Milli(1735690500000, 'UTC') OR num != 1) SETTINGS optimize_move_to_prewhere = 1;
SELECT 'prewhere not shifted', count() FROM tab WHERE NOT (time < fromUnixTimestamp64Milli(1735689700003, 'UTC') OR time > fromUnixTimestamp64Milli(1735690500003, 'UTC') OR num != 1) SETTINGS optimize_move_to_prewhere = 1;
-- A granule recorded as not matching an earlier variant is read if it has rows in the band: the earlier
-- bounds exclude the matching rows, the new ones include them.
SELECT 'prewhere before matches', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689700000, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690000000, 'UTC') AND num = 1 SETTINGS optimize_move_to_prewhere = 1;
SELECT 'prewhere including matches', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689700000, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690200000, 'UTC') AND num = 1 SETTINGS optimize_move_to_prewhere = 1;
-- Other conditions do not share entries.
SELECT 'prewhere other condition', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689700003, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690500003, 'UTC') AND num = 0 SETTINGS optimize_move_to_prewhere = 1;

-- Disabled: the shifted bounds do not use the entries of the earlier ones.
SELECT 'disabled', count() FROM tab WHERE time >= fromUnixTimestamp64Milli(1735689700006, 'UTC') AND time <= fromUnixTimestamp64Milli(1735690500006, 'UTC') AND num = 1 SETTINGS use_query_condition_cache_for_changing_bounds = 0;

-- A window relative to the current time moves on both sides with every query.
TRUNCATE TABLE tab;
INSERT INTO tab SELECT now64(3, 'UTC') - toIntervalMillisecond(number), number IN (500000, 500001)
FROM numbers(1000000)
SETTINGS max_insert_threads = 1, max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
SELECT 'now prime', count() FROM tab WHERE time >= now64(3, 'UTC') - INTERVAL 900 SECOND AND time <= now64(3, 'UTC') - INTERVAL 100 SECOND AND num = 1;
SELECT 'now moved', count() FROM tab WHERE time >= now64(3, 'UTC') - INTERVAL 900 SECOND AND time <= now64(3, 'UTC') - INTERVAL 100 SECOND AND num = 1;

-- The number of granules each query read. With the condition in `WHERE`, a query reusing the cache reads a few
-- granules around the matching rows, like a repetition of the same query does: not every granule without
-- matching rows in a block with matching rows is recorded.
SYSTEM FLUSH LOGS query_log;
SELECT
    name,
    if(granules BETWEEN min_granules AND max_granules,
        'granules as expected',
        format('read {} granules, expected from {} to {}', granules, min_granules, max_granules))
FROM
(
    SELECT extract(query, 'SELECT \'([^\']+)\', count\(\) FROM tab') AS name, intDiv(any(read_rows) + 8191, 8192) AS granules
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
        AND type = 'QueryFinish' AND current_database = currentDatabase() AND name != ''
    GROUP BY name
) AS log
INNER JOIN
(
    SELECT * FROM VALUES('name String, min_granules UInt64, max_granules UInt64',
        ('where prime', 96, 100),
        ('where shifted', 1, 12),
        ('where narrower', 1, 12),
        ('where wider', 1, 35),
        ('where string', 102, 106),
        ('where string shifted', 1, 12),
        ('where not', 96, 100),
        ('where not shifted', 1, 12),
        ('where before matches', 1, 12),
        ('where including matches', 25, 40),
        ('where other condition', 96, 100),
        ('prewhere prime', 96, 100),
        ('prewhere shifted', 1, 12),
        ('prewhere narrower', 1, 12),
        ('prewhere wider', 1, 35),
        ('prewhere string', 102, 106),
        ('prewhere string shifted', 1, 12),
        ('prewhere not', 96, 100),
        ('prewhere not shifted', 1, 12),
        ('prewhere before matches', 1, 12),
        ('prewhere including matches', 25, 40),
        ('prewhere other condition', 96, 100),
        ('disabled', 96, 100),
        ('now prime', 80, 100),
        ('now moved', 1, 12))
) AS expected USING (name)
ORDER BY name;

DROP TABLE tab;
