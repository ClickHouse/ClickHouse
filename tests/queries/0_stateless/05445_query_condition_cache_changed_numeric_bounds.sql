-- Tags: no-parallel-replicas
-- no-parallel-replicas: the query condition cache is populated per replica

-- The query condition cache serves a condition whose numeric bounds changed since it was cached: a granule
-- recorded as not matching an earlier variant of the condition is skipped unless the primary key shows that
-- it may have rows admitted by the new bounds but not by the earlier ones. With narrower bounds, the primary
-- key is not needed.

-- The cache records granules only from reads that are not split into layers. Granules are computed from
-- `read_rows`, so their size has to be fixed. Small gaps between ranges must not be read.
SET use_query_condition_cache = 1, enable_analyzer = 1,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0;

-- 123 granules, a single part. The only matching rows (`num = 1`) are in the granules of the rows 500999
-- and 501000. Only the first one has a high `price`, `amount` and `qty`.
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (key UInt64, price Float64, amount Decimal(10, 2), qty UInt32, num UInt64) ENGINE = MergeTree ORDER BY key
    SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', add_minmax_index_for_numeric_columns = 0;
INSERT INTO tab SELECT number, (number % 1000) / 10, (number % 1000) / 10, number % 1000, number IN (500999, 501000)
FROM numbers(1000000)
SETTINGS max_insert_threads = 1, max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

-- Bounds on the primary key: 98 granules, then the bounds moved by 10 rows.
SELECT 'key prime', count() FROM tab WHERE key > 100000 AND key < 900000 AND num = 1;
SELECT 'key shifted', count() FROM tab WHERE key > 100010 AND key < 900010 AND num = 1;
-- The band contains the matching rows.
SELECT 'key before matches', count() FROM tab WHERE key > 100000 AND key < 400000 AND num = 1;
SELECT 'key including matches', count() FROM tab WHERE key > 100000 AND key < 600000 AND num = 1;

-- A threshold on a column outside of the primary key: only narrower thresholds reuse the cache.
SELECT 'float prime', count() FROM tab WHERE price > 50.5 AND num = 1;
SELECT 'float narrower', count() FROM tab WHERE price > 60.5 AND num = 1;
SELECT 'float wider', count() FROM tab WHERE price > 40 AND num = 1;

-- The literals `100` and `300` have different types (`UInt8` and `UInt16`).
SELECT 'integer prime', count() FROM tab WHERE qty > 100 AND num = 1;
SELECT 'integer narrower', count() FROM tab WHERE qty > 300 AND num = 1;

SELECT 'decimal prime', count() FROM tab WHERE amount >= 50.25 AND num = 1;
SELECT 'decimal narrower', count() FROM tab WHERE amount >= 60.75 AND num = 1;

-- A comparison with NaN is never true, so it is not a bound: it must not reuse the entries of other
-- thresholds, nor provide entries for them.
SELECT 'nan prime', count() FROM tab WHERE price > 10 OR num = 1;
SELECT 'nan', count() FROM tab WHERE price > nan OR num = 1;
SELECT 'nan reversed prime', count() FROM tab WHERE price < nan OR num = 1;
SELECT 'nan reversed', count() FROM tab WHERE price < 0 OR num = 1;

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
        ('key prime', 96, 100),
        ('key shifted', 1, 12),
        ('key before matches', 1, 12),
        ('key including matches', 25, 40),
        ('float prime', 120, 123),
        ('float narrower', 1, 12),
        ('float wider', 120, 123),
        ('integer prime', 120, 123),
        ('integer narrower', 1, 12),
        ('decimal prime', 120, 123),
        ('decimal narrower', 1, 12),
        ('nan prime', 120, 123),
        ('nan', 120, 123),
        ('nan reversed prime', 120, 123),
        ('nan reversed', 120, 123))
) AS expected USING (name)
ORDER BY name;

DROP TABLE tab;
