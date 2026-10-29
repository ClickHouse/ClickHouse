-- GROUP BY a LowCardinality key with count() alone keeps the count in the hash table and counts the rows of every
-- dictionary position of a block. Every result is compared with the same aggregation over the key cast to its nested
-- type, under settings that change the blocks, the table layout and the conversion.

SET max_threads = 4;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_lc_count;
CREATE TABLE t_lc_count
(
    s LowCardinality(String),
    sn LowCardinality(Nullable(String)),
    u LowCardinality(UInt32),
    f LowCardinality(FixedString(4)),
    n UInt64
)
ENGINE = MergeTree ORDER BY n
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_lc_count
SELECT
    toString(cityHash64(number) % 25000),
    if(number % 7 = 0, NULL, toString(number % 3000)),
    toUInt32(number % 1000),
    toFixedString(toString(number % 300), 4),
    number
FROM numbers(100000)
SETTINGS max_block_size = 65536;

SELECT 'string',
    (SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM t_lc_count GROUP BY s))
    = (SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT CAST(s AS String) AS s, count() AS c FROM t_lc_count GROUP BY s));

SELECT 'nullable',
    (SELECT count(), sum(c), sum(cityHash64(isNull(sn), ifNull(sn, ''), c)) FROM (SELECT sn, count() AS c FROM t_lc_count GROUP BY sn))
    = (SELECT count(), sum(c), sum(cityHash64(isNull(sn), ifNull(sn, ''), c)) FROM (SELECT CAST(sn AS Nullable(String)) AS sn, count() AS c FROM t_lc_count GROUP BY sn));

SELECT 'number',
    (SELECT count(), sum(c), sum(cityHash64(u, c)) FROM (SELECT u, count() AS c FROM t_lc_count GROUP BY u))
    = (SELECT count(), sum(c), sum(cityHash64(u, c)) FROM (SELECT CAST(u AS UInt32) AS u, count() AS c FROM t_lc_count GROUP BY u));

SELECT 'fixed_string',
    (SELECT count(), sum(c), sum(cityHash64(f, c)) FROM (SELECT f, count() AS c FROM t_lc_count GROUP BY f))
    = (SELECT count(), sum(c), sum(cityHash64(f, c)) FROM (SELECT CAST(f AS FixedString(4)) AS f, count() AS c FROM t_lc_count GROUP BY f));

-- Another aggregate next to count() keeps the count state.
SELECT 'count_and_sum',
    (SELECT count(), sum(c), sum(cityHash64(s, c, m)) FROM (SELECT s, count() AS c, sum(n) AS m FROM t_lc_count GROUP BY s))
    = (SELECT count(), sum(c), sum(cityHash64(s, c, m)) FROM (SELECT CAST(s AS String) AS s, count() AS c, sum(n) AS m FROM t_lc_count GROUP BY s));

SELECT 'small_blocks',
    (SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM t_lc_count GROUP BY s) SETTINGS max_block_size = 1000)
    = (SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT CAST(s AS String) AS s, count() AS c FROM t_lc_count GROUP BY s));

SELECT 'two_level',
    (SELECT count(), sum(c), sum(cityHash64(isNull(sn), ifNull(sn, ''), c)) FROM (SELECT sn, count() AS c FROM t_lc_count GROUP BY sn)
        SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1)
    = (SELECT count(), sum(c), sum(cityHash64(isNull(sn), ifNull(sn, ''), c)) FROM (SELECT CAST(sn AS Nullable(String)) AS sn, count() AS c FROM t_lc_count GROUP BY sn));

SELECT 'external',
    (SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM t_lc_count GROUP BY s)
        SETTINGS max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0, group_by_two_level_threshold = 1)
    = (SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT CAST(s AS String) AS s, count() AS c FROM t_lc_count GROUP BY s));

SELECT 'no_prefetch',
    (SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM t_lc_count GROUP BY s) SETTINGS enable_software_prefetch_in_aggregation = 0)
    = (SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT CAST(s AS String) AS s, count() AS c FROM t_lc_count GROUP BY s));

-- A key made by a function in one block of 100000 rows has 32-bit dictionary positions.
SELECT 'function_key',
    (SELECT count(), sum(c), sum(cityHash64(k, c)) FROM (SELECT toLowCardinality(toString(number % 70000)) AS k, count() AS c FROM numbers(100000) GROUP BY k) SETTINGS max_block_size = 100000)
    = (SELECT count(), sum(c), sum(cityHash64(k, c)) FROM (SELECT toString(number % 70000) AS k, count() AS c FROM numbers(100000) GROUP BY k));

-- With one thread the table outgrows the L2 cache, and the later blocks prefetch the cells of their positions.
SELECT 'prefetch',
    (SELECT count(), sum(c), sum(cityHash64(k, c)) FROM (SELECT toLowCardinality(toString(intDiv(number, 2) % 50000)) AS k, count() AS c FROM numbers(200000) GROUP BY k)
        SETTINGS max_block_size = 1000, max_threads = 1, enable_software_prefetch_in_aggregation = 1)
    = (SELECT count(), sum(c), sum(cityHash64(k, c)) FROM (SELECT toString(intDiv(number, 2) % 50000) AS k, count() AS c FROM numbers(200000) GROUP BY k));

-- With one thread and no spill, a group that is kept after the table stops taking new keys counts all its rows.
SELECT 'no_more_keys', count() > 0, countIf(c != tc)
FROM
(
    SELECT s, count() AS c FROM t_lc_count GROUP BY s
    SETTINGS max_rows_to_group_by = 1000, group_by_overflow_mode = 'any', max_threads = 1,
        max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0, enable_parallel_replicas = 0
) AS a
INNER JOIN (SELECT CAST(s AS String) AS s, count() AS tc FROM t_lc_count GROUP BY s) AS b USING (s);

-- With WITH TOTALS and totals_mode = 'before_having', the rows of the groups the table no longer takes are counted in the
-- overflow row, so the totals still count every row.
SELECT 'overflow_row';
SELECT s, count() AS c FROM t_lc_count GROUP BY s WITH TOTALS HAVING c > 1000000
SETTINGS max_rows_to_group_by = 1000, group_by_overflow_mode = 'any', totals_mode = 'before_having', max_threads = 1,
    max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0, enable_parallel_replicas = 0;

-- The partial results of the shards are merged by key.
SELECT 'remote',
    (SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT s, count() AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_lc_count) GROUP BY s))
    = (SELECT count(), sum(c), sum(cityHash64(s, c)) FROM (SELECT CAST(s AS String) AS s, count() AS c FROM remote('127.0.0.{1,2}', currentDatabase(), t_lc_count) GROUP BY s));

-- A LowCardinality key packed with another fixed key.
SELECT 'keys128',
    (SELECT count(), sum(c), sum(cityHash64(u, m, c)) FROM (SELECT u, toUInt32(n % 7) AS m, count() AS c FROM t_lc_count GROUP BY u, m))
    = (SELECT count(), sum(c), sum(cityHash64(u, m, c)) FROM (SELECT CAST(u AS UInt32) AS u, toUInt32(n % 7) AS m, count() AS c FROM t_lc_count GROUP BY u, m));

-- The GROUP BY top-K heap ranks the rows one by one and keeps the count state. CI's profile sets max_rows_to_group_by
-- and randomizes the top-K limit, which turn the heap off, so both are pinned and the plan is checked.
SELECT 'top_k_heap',
    (SELECT groupArray((s, c)) FROM (SELECT s, count() AS c FROM t_lc_count GROUP BY s ORDER BY s LIMIT 10 SETTINGS enable_group_by_top_k_optimization = 1, max_rows_to_group_by = 0, query_plan_max_limit_for_top_k_optimization = 1000, serialize_query_plan = 0, enable_parallel_replicas = 0))
    = (SELECT groupArray((s, c)) FROM (SELECT CAST(s AS String) AS s, count() AS c FROM t_lc_count GROUP BY s ORDER BY s LIMIT 10 SETTINGS enable_group_by_top_k_optimization = 0));
SELECT 'top_k_heap_plan', count() > 0 FROM (EXPLAIN actions = 1
    SELECT s, count() AS c FROM t_lc_count GROUP BY s ORDER BY s LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1, max_rows_to_group_by = 0, query_plan_max_limit_for_top_k_optimization = 1000, serialize_query_plan = 0, enable_parallel_replicas = 0)
WHERE explain LIKE '%Top-K%';

DROP TABLE t_lc_count;
