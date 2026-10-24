-- Tags: no-parallel-replicas
-- no-parallel-replicas: the query condition cache is populated per replica

-- With `use_primary_key = 0`, the query condition cache does not use the primary key to check whether a
-- granule may have rows admitted by changed bounds of a condition but not by the earlier ones: wider bounds
-- read every granule. Narrower bounds need no primary key and still reuse the entries of the earlier bounds.

SET use_query_condition_cache = 1, enable_analyzer = 1,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0;

-- 123 granules, a single part. The only matching rows (`num = 1`) are in the granules of the rows 500999 and 501000.
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (key UInt64, num UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
INSERT INTO tab SELECT number, number IN (500999, 501000)
FROM numbers(1000000)
SETTINGS max_insert_threads = 1, max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

-- 98 granules are read and recorded.
SELECT 'prime', count() FROM tab WHERE key > 100000 AND key < 900000 AND num = 1;
-- Narrower bounds: the band is empty, so the entries of the earlier bounds are used without the primary key.
SELECT 'narrower', count() FROM tab WHERE key > 100000 AND key < 899990 AND num = 1 SETTINGS use_primary_key = 0;
-- Wider bounds: the band has to be checked with the primary key, which is disabled, so none of the
-- earlier variants can be used.
SELECT 'wider', count() FROM tab WHERE key > 100000 AND key < 900010 AND num = 1 SETTINGS use_primary_key = 0;

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
        ('prime', 96, 100),
        ('wider', 120, 123),
        ('narrower', 1, 12))
) AS expected USING (name)
ORDER BY name;

DROP TABLE tab;
