-- A hash join that may rerange its right table by key does not build a row store, since the rerange rewrites the
-- stored columns. The right side here has 50 rows per key and a wide payload that would otherwise go to a row store.
-- With the rerange the results equal those without it.

SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET collect_hash_table_stats_during_joins = 0;
SET enable_hash_join_row_store = 1, min_rows_ratio_for_hash_join_row_store = 0;
SET join_to_sort_minimum_perkey_rows = 40, join_to_sort_maximum_table_rows = 10000;
SET max_threads = 4;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY ();
CREATE TABLE r (k UInt64, a UInt64, b String, c Float64, d Date, e UInt32, f String) ENGINE = MergeTree ORDER BY ();
INSERT INTO l SELECT number % 25, number FROM numbers(100);
INSERT INTO r SELECT number % 20, number, toString(number * 7), number / 3, toDate('2026-01-01') + number % 300, number * 13, repeat('x', number % 9)
FROM numbers(1000);

SELECT kind, layout, sorted = unsorted FROM
(
    SELECT 'ALL INNER' AS kind, 'serial' AS layout,
        (SELECT (count(), sum(cityHash64(l.v, r.a, r.b, r.c, r.d, r.e, r.f))) FROM l ALL INNER JOIN r ON l.k = r.k
            SETTINGS allow_join_right_table_sorting = 1, hash_join_layout = 'serial') AS sorted,
        (SELECT (count(), sum(cityHash64(l.v, r.a, r.b, r.c, r.d, r.e, r.f))) FROM l ALL INNER JOIN r ON l.k = r.k
            SETTINGS allow_join_right_table_sorting = 0, hash_join_layout = 'serial') AS unsorted
    UNION ALL
    SELECT 'ALL INNER', 'parallel',
        (SELECT (count(), sum(cityHash64(l.v, r.a, r.b, r.c, r.d, r.e, r.f))) FROM l ALL INNER JOIN r ON l.k = r.k
            SETTINGS allow_join_right_table_sorting = 1, hash_join_layout = 'parallel'),
        (SELECT (count(), sum(cityHash64(l.v, r.a, r.b, r.c, r.d, r.e, r.f))) FROM l ALL INNER JOIN r ON l.k = r.k
            SETTINGS allow_join_right_table_sorting = 0, hash_join_layout = 'parallel')
    UNION ALL
    SELECT 'ALL LEFT', 'serial',
        (SELECT (count(), sum(cityHash64(l.v, r.a, r.b, r.c, r.d, r.e, r.f))) FROM l ALL LEFT JOIN r ON l.k = r.k
            SETTINGS allow_join_right_table_sorting = 1, hash_join_layout = 'serial'),
        (SELECT (count(), sum(cityHash64(l.v, r.a, r.b, r.c, r.d, r.e, r.f))) FROM l ALL LEFT JOIN r ON l.k = r.k
            SETTINGS allow_join_right_table_sorting = 0, hash_join_layout = 'serial')
    UNION ALL
    SELECT 'ALL LEFT', 'parallel',
        (SELECT (count(), sum(cityHash64(l.v, r.a, r.b, r.c, r.d, r.e, r.f))) FROM l ALL LEFT JOIN r ON l.k = r.k
            SETTINGS allow_join_right_table_sorting = 1, hash_join_layout = 'parallel'),
        (SELECT (count(), sum(cityHash64(l.v, r.a, r.b, r.c, r.d, r.e, r.f))) FROM l ALL LEFT JOIN r ON l.k = r.k
            SETTINGS allow_join_right_table_sorting = 0, hash_join_layout = 'parallel')
)
ORDER BY kind, layout;

-- Some of the joined rows, with the rerange.
SELECT l.v, r.a, r.b, r.f FROM l ALL INNER JOIN r ON l.k = r.k WHERE l.v < 3 AND r.a < 100 ORDER BY l.v, r.a
SETTINGS allow_join_right_table_sorting = 1, hash_join_layout = 'serial';

DROP TABLE l;
DROP TABLE r;
