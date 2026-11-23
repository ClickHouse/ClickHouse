-- The prefetching loops of the aggregation keep the keys that they build ahead of the current row to prefetch the
-- cells of these keys, and insert the kept keys, when the key stores its hash: a single `String` key
-- (`key_packed_string`), and fixed-width keys with one `String` or `LowCardinality(String)` key
-- (`keys32_packed_string`, `keys64_packed_string`). The aggregation is single-threaded and its hash table outgrows the
-- CPU cache, so the aggregator prefetches. Each case compares the result of one prefetching loop with the result of
-- the serialized method without prefetching: the loop with aggregate functions, the one without them, and the loop of
-- a lone `count`, with and without a top-K heap.

SET max_threads = 1;
SET enable_adaptive_aggregator = 0;
SET optimize_aggregation_in_order = 0;
SET collect_hash_table_stats_during_aggregation = 0;
SET group_by_two_level_threshold = 0;
SET group_by_two_level_threshold_bytes = 0;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;
SET enable_group_by_top_k_optimization = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
-- The profile of the test server limits the number of groups, and an aggregation with such a limit keeps no top-K
-- heap. The distributed-plan suite serializes every plan, which the top-K plan passes refuse.
SET max_rows_to_group_by = 0;
SET serialize_query_plan = 0;

DROP VIEW IF EXISTS v_05618;

-- 600 thousand strings: empty, kept inline, and stored out of line.
CREATE VIEW v_05618 AS
SELECT
    toUInt32(number % 1000) AS a,
    number % 1000 * 1000003 AS d,
    multiIf(number % 11 = 0, '', number % 11 = 1, concat(repeat('x', 30 + number % 20), toString(number)), toString(number)) AS s,
    number AS x
FROM numbers(600000);

SELECT 'key_packed_string, sum',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT s, sum(x) FROM v_05618 GROUP BY s)
        SETTINGS enable_software_prefetch_in_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT s, sum(x) FROM v_05618 GROUP BY s)
        SETTINGS enable_software_prefetch_in_aggregation = 0, enable_packed_string_keys_in_aggregation = 0);

SELECT 'key_packed_string, no aggregate functions',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT s FROM v_05618 GROUP BY s)
        SETTINGS enable_software_prefetch_in_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT s FROM v_05618 GROUP BY s)
        SETTINGS enable_software_prefetch_in_aggregation = 0, enable_packed_string_keys_in_aggregation = 0);

SELECT 'key_packed_string, count',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT s, count() FROM v_05618 GROUP BY s)
        SETTINGS enable_software_prefetch_in_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT s, count() FROM v_05618 GROUP BY s)
        SETTINGS enable_software_prefetch_in_aggregation = 0, enable_packed_string_keys_in_aggregation = 0);

SELECT 'key_packed_string, count, ORDER BY keys LIMIT',
    (SELECT groupArray(tuple(*)) FROM (SELECT s, count() FROM v_05618 GROUP BY s ORDER BY s LIMIT 10)
        SETTINGS enable_software_prefetch_in_aggregation = 1)
    = (SELECT groupArray(tuple(*)) FROM (SELECT s, count() FROM v_05618 GROUP BY s ORDER BY s LIMIT 10)
        SETTINGS enable_software_prefetch_in_aggregation = 0, enable_packed_string_keys_in_aggregation = 0,
            enable_group_by_top_k_optimization = 0);

SELECT 'keys32_packed_string, sum',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, sum(x) FROM v_05618 GROUP BY a, s)
        SETTINGS enable_software_prefetch_in_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, sum(x) FROM v_05618 GROUP BY a, s)
        SETTINGS enable_software_prefetch_in_aggregation = 0, enable_fixed_and_packed_string_keys_in_aggregation = 0);

SELECT 'keys32_packed_string, no aggregate functions',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s FROM v_05618 GROUP BY a, s)
        SETTINGS enable_software_prefetch_in_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s FROM v_05618 GROUP BY a, s)
        SETTINGS enable_software_prefetch_in_aggregation = 0, enable_fixed_and_packed_string_keys_in_aggregation = 0);

SELECT 'keys32_packed_string, count',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count() FROM v_05618 GROUP BY a, s)
        SETTINGS enable_software_prefetch_in_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, s, count() FROM v_05618 GROUP BY a, s)
        SETTINGS enable_software_prefetch_in_aggregation = 0, enable_fixed_and_packed_string_keys_in_aggregation = 0);

SELECT 'keys32_packed_string, count, ORDER BY keys LIMIT',
    (SELECT groupArray(tuple(*)) FROM (SELECT a, s, count() FROM v_05618 GROUP BY a, s ORDER BY a DESC, s LIMIT 10)
        SETTINGS enable_software_prefetch_in_aggregation = 1)
    = (SELECT groupArray(tuple(*)) FROM (SELECT a, s, count() FROM v_05618 GROUP BY a, s ORDER BY a DESC, s LIMIT 10)
        SETTINGS enable_software_prefetch_in_aggregation = 0, enable_fixed_and_packed_string_keys_in_aggregation = 0,
            enable_group_by_top_k_optimization = 0);

SELECT 'keys64_packed_string, sum, two levels',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, s, sum(x) FROM v_05618 GROUP BY d, s)
        SETTINGS enable_software_prefetch_in_aggregation = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT d, s, sum(x) FROM v_05618 GROUP BY d, s)
        SETTINGS enable_software_prefetch_in_aggregation = 0, enable_fixed_and_packed_string_keys_in_aggregation = 0);

-- The look-ahead reads the string of a `LowCardinality(String)` key from the dictionary of the block.
SELECT 'keys32_packed_string, LowCardinality(String), sum',
    (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, toLowCardinality(s) AS ls, sum(x) FROM v_05618 GROUP BY a, ls)
        SETTINGS enable_software_prefetch_in_aggregation = 1)
    = (SELECT count(), sum(cityHash64(*)) FROM (SELECT a, toLowCardinality(s) AS ls, sum(x) FROM v_05618 GROUP BY a, ls)
        SETTINGS enable_software_prefetch_in_aggregation = 0, enable_fixed_and_packed_string_keys_in_aggregation = 0);

DROP VIEW v_05618;
