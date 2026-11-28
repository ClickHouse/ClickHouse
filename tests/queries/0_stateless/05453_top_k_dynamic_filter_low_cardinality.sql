-- Tags: no-fasttest
-- Tag no-fasttest: COLLATE needs ICU

-- The top-K dynamic filter (`__topKFilter`) compares the dictionary values of a `LowCardinality` column once per block
-- (or only the values the block uses, when the dictionary is much larger than the block) and maps the rows through the
-- indexes. The sort key values of every result must stay the same as without the filter, also for `NULL`, `COLLATE`,
-- ties, rows equal to the threshold, a nested `LowCardinality`, a `Memory` table and `GROUP BY key ORDER BY key LIMIT n`.

DROP TABLE IF EXISTS t_lc;
DROP TABLE IF EXISTS t_lc_memory;

CREATE TABLE t_lc (id UInt64, s LowCardinality(String), sn LowCardinality(Nullable(String)), t LowCardinality(String),
    d LowCardinality(String), fs LowCardinality(FixedString(16)), tup Tuple(LowCardinality(String), UInt32))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_lc;

-- Three wide parts, so three dictionaries shared by all granules of a part (a compact part has one per granule). `s` is empty in about 60% of the rows, else one of 2000 values, so that a part's
-- dictionary is more than 8 times a block of 64 rows and less than 8 times a block of 1000 rows. `sn` is `s` with `NULL`
-- in a third of the rows. `t` has 50 values of about 400 rows each, scattered. `d` is distinct and decreases with `id`.
INSERT INTO t_lc SELECT number,
    if(cityHash64(number) % 10 < 6, '', concat('v', toString(cityHash64(number, 1) % 2000))) AS s,
    if(number % 3 = 0, NULL, s), concat('t', toString(cityHash64(number, 2) % 50)), concat('d', leftPad(toString(100000 - number), 6, '0')),
    toFixedString(concat('f', toString(cityHash64(number, 3) % 300)), 16), (s, toUInt32(number % 7))
FROM numbers(0, 7000);
INSERT INTO t_lc SELECT number,
    if(cityHash64(number) % 10 < 6, '', concat('v', toString(cityHash64(number, 1) % 2000))) AS s,
    if(number % 3 = 0, NULL, s), concat('t', toString(cityHash64(number, 2) % 50)), concat('d', leftPad(toString(100000 - number), 6, '0')),
    toFixedString(concat('f', toString(cityHash64(number, 3) % 300)), 16), (s, toUInt32(number % 7))
FROM numbers(7000, 7000);
INSERT INTO t_lc SELECT number,
    if(cityHash64(number) % 10 < 6, '', concat('v', toString(cityHash64(number, 1) % 2000))) AS s,
    if(number % 3 = 0, NULL, s), concat('t', toString(cityHash64(number, 2) % 50)), concat('d', leftPad(toString(100000 - number), 6, '0')),
    toFixedString(concat('f', toString(cityHash64(number, 3) % 300)), 16), (s, toUInt32(number % 7))
FROM numbers(14000, 7000);

SELECT 'parts', count(), min(c) > 1000, max(c) < 8000 FROM (SELECT uniqExact(s) AS c FROM t_lc GROUP BY _part);

CREATE TABLE t_lc_memory (s LowCardinality(String)) ENGINE = Memory;
-- 20 blocks: the filter compares the later ones with the threshold of the earlier ones.
INSERT INTO t_lc_memory SELECT concat('m', toString(cityHash64(number) % 2000)) FROM numbers(20000)
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

SELECT 'plan s', count() > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM t_lc ORDER BY s LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0
) WHERE explain LIKE '%__topKFilter%';
SELECT 'plan sn', count() > 0 FROM (
    EXPLAIN actions = 1 SELECT sn FROM t_lc ORDER BY sn LIMIT 5
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0
) WHERE explain LIKE '%__topKFilter%';
SELECT 'plan memory', count() > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM t_lc_memory ORDER BY s LIMIT 30
    SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, use_top_k_dynamic_filtering_for_variable_length_types = 1, max_threads = 1
) WHERE explain LIKE '%TopN filter column%';
SELECT 'plan group by', count() > 0 FROM (
    EXPLAIN actions = 1 SELECT s FROM t_lc GROUP BY s ORDER BY s LIMIT 5
    SETTINGS enable_group_by_top_k_optimization = 1, enable_group_by_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_variable_length_types = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 64, max_threads = 1, max_rows_to_group_by = 0, enable_parallel_replicas = 0
) WHERE explain LIKE '%__topKFilter%';

-- Each check prints whether the sort key values of the result, in order (as tuples, which keep `NULL`), are equal to those
-- of the same query without the filter, and their number.
-- max_threads = 1 on the filter side: the later blocks are read only after a threshold is published.
SELECT 's asc', 64, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 64, max_threads = 1) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 's asc', 1000, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 1000, max_threads = 1) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 's desc', 64, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s DESC LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 64, max_threads = 1) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s DESC LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 's desc', 1000, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s DESC LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 1000, max_threads = 1) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s DESC LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 's where', 64, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc WHERE s != '' ORDER BY s LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 64, max_threads = 1) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc WHERE s != '' ORDER BY s LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 's where', 1000, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc WHERE s != '' ORDER BY s LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 1000, max_threads = 1) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc WHERE s != '' ORDER BY s LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 'sn nulls first, NULL threshold', 64, a = b, length(a), countEqual(a, tuple(NULL)) FROM (SELECT
    (SELECT groupArray(tuple(sn)) FROM (SELECT sn FROM t_lc ORDER BY sn NULLS FIRST LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 64) AS a,
    (SELECT groupArray(tuple(sn)) FROM (SELECT sn FROM t_lc ORDER BY sn NULLS FIRST LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 'sn nulls first, NULL threshold', 1000, a = b, length(a), countEqual(a, tuple(NULL)) FROM (SELECT
    (SELECT groupArray(tuple(sn)) FROM (SELECT sn FROM t_lc ORDER BY sn NULLS FIRST LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 1000, max_threads = 1) AS a,
    (SELECT groupArray(tuple(sn)) FROM (SELECT sn FROM t_lc ORDER BY sn NULLS FIRST LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 'sn desc nulls last', 64, a = b, length(a), countEqual(a, tuple(NULL)) FROM (SELECT
    (SELECT groupArray(tuple(sn)) FROM (SELECT sn FROM t_lc ORDER BY sn DESC NULLS LAST LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 64, max_threads = 1) AS a,
    (SELECT groupArray(tuple(sn)) FROM (SELECT sn FROM t_lc ORDER BY sn DESC NULLS LAST LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 'sn desc nulls last', 1000, a = b, length(a), countEqual(a, tuple(NULL)) FROM (SELECT
    (SELECT groupArray(tuple(sn)) FROM (SELECT sn FROM t_lc ORDER BY sn DESC NULLS LAST LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 1000, max_threads = 1) AS a,
    (SELECT groupArray(tuple(sn)) FROM (SELECT sn FROM t_lc ORDER BY sn DESC NULLS LAST LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 's collate', 64, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s COLLATE 'en' LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 64, max_threads = 1) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s COLLATE 'en' LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 's collate', 1000, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s COLLATE 'en' LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 1000, max_threads = 1) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s COLLATE 'en' LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 's collate desc', 64, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s DESC COLLATE 'en' LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 64, max_threads = 1) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s DESC COLLATE 'en' LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 's collate desc', 1000, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s DESC COLLATE 'en' LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 1000, max_threads = 1) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc ORDER BY s DESC COLLATE 'en' LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 't ties', 64, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_lc ORDER BY t LIMIT 1000) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 64, max_threads = 1) AS a,
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_lc ORDER BY t LIMIT 1000) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 't ties', 1000, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_lc ORDER BY t LIMIT 1000) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 1000, max_threads = 1) AS a,
    (SELECT groupArray(tuple(t)) FROM (SELECT t FROM t_lc ORDER BY t LIMIT 1000) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 'd', 64, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(d)) FROM (SELECT d FROM t_lc ORDER BY d LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 64, max_threads = 1) AS a,
    (SELECT groupArray(tuple(d)) FROM (SELECT d FROM t_lc ORDER BY d LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 'd', 1000, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(d)) FROM (SELECT d FROM t_lc ORDER BY d LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 1000, max_threads = 1) AS a,
    (SELECT groupArray(tuple(d)) FROM (SELECT d FROM t_lc ORDER BY d LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 'fs', 64, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(fs)) FROM (SELECT fs FROM t_lc ORDER BY fs LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 64, max_threads = 1) AS a,
    (SELECT groupArray(tuple(fs)) FROM (SELECT fs FROM t_lc ORDER BY fs LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 'fs', 1000, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(fs)) FROM (SELECT fs FROM t_lc ORDER BY fs LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, max_block_size = 1000, max_threads = 1) AS a,
    (SELECT groupArray(tuple(fs)) FROM (SELECT fs FROM t_lc ORDER BY fs LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 0) AS b);

-- `LowCardinality` nested in a `Tuple` is compared without `LowCardinality`.
SELECT 'tup', 64, a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(tup)) FROM (SELECT tup FROM t_lc ORDER BY tup LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, use_top_k_dynamic_filtering_for_variable_length_types = 1, max_block_size = 64, max_threads = 1) AS a,
    (SELECT groupArray(tuple(tup)) FROM (SELECT tup FROM t_lc ORDER BY tup LIMIT 20) SETTINGS use_top_k_dynamic_filtering = 0) AS b);

-- A `Memory` table, whose blocks are read whole.
SELECT 'memory', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc_memory ORDER BY s LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, use_top_k_dynamic_filtering_for_variable_length_types = 1, max_threads = 1) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc_memory ORDER BY s LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);
SELECT 'memory desc', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc_memory ORDER BY s DESC LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 1, use_top_k_dynamic_filtering_for_single_string_sort_column = 1, query_plan_max_limit_for_top_k_optimization = 100000, use_query_condition_cache = 0, use_top_k_dynamic_filtering_for_variable_length_types = 1, max_threads = 1) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc_memory ORDER BY s DESC LIMIT 30) SETTINGS use_top_k_dynamic_filtering = 0) AS b);

-- The CI profile sets `max_rows_to_group_by`, which disables the GROUP BY top-K optimization, and the GROUP BY
-- dynamic filter is not applied to reads from parallel replicas.
SELECT 'group by', a = b, length(a) FROM (SELECT
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc GROUP BY s ORDER BY s LIMIT 5)
        SETTINGS enable_group_by_top_k_optimization = 1, enable_group_by_top_k_dynamic_filtering = 1,
        use_top_k_dynamic_filtering_for_variable_length_types = 1, query_plan_max_limit_for_top_k_optimization = 100000,
        use_query_condition_cache = 0, max_block_size = 64, max_threads = 1, max_rows_to_group_by = 0,
        enable_parallel_replicas = 0) AS a,
    (SELECT groupArray(tuple(s)) FROM (SELECT s FROM t_lc GROUP BY s ORDER BY s LIMIT 5)
        SETTINGS enable_group_by_top_k_optimization = 0) AS b);

DROP TABLE t_lc;
DROP TABLE t_lc_memory;
