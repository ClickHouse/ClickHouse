-- A two-level `GROUP BY` table that holds few keys decides the software prefetch by its keys, not by the preallocated
-- buckets (see `bytesForPrefetch`). The decision never changes the result, so the test only checks that every two-level
-- table shape behind that gate returns the same rows with the prefetch on and off: the issue's
-- `LowCardinality(Nullable(String))` key, `Nullable(String)` (`TwoLevelStringHashMap`), `keys256`, serialized keys
-- (`HashMethodSerialized` has a gate of its own), and a two-level table that has outgrown its initial allocation.

DROP TABLE IF EXISTS t_two_level_few_keys;

CREATE TABLE t_two_level_few_keys
(
    lc LowCardinality(Nullable(String)),
    s Nullable(String),
    str String,
    a UInt64,
    b UInt64,
    c UInt64,
    many UInt64
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_two_level_few_keys
SELECT v, v, ifNull(v, ''), intHash32(intDiv(number, 16)) % 13, 7, 11, number % 100000
FROM
(
    SELECT
        number,
        multiIf(
            intHash32(intDiv(number, 16)) % 100 < 40, 'category_value_01_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            intHash32(intDiv(number, 16)) % 100 < 70, 'category_value_02_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
            intHash32(intDiv(number, 16)) % 100 < 95, 'category_value_03_cccccccccccccccccccccccccccccccccccccccc',
            intHash32(intDiv(number, 16)) % 100 < 97, NULL,
            concat('category_value_', toString((intHash32(number) % 9) + 4), '_dddddddddddddddddddddddddddddddddddddddd')) AS v
    FROM numbers_mt(300000)
);

SET max_threads = 8, max_block_size = 8192, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1;

SELECT 'LowCardinality(Nullable(String)), prefetch on';
SELECT lc, count() FROM t_two_level_few_keys GROUP BY lc ORDER BY lc SETTINGS enable_software_prefetch_in_aggregation = 1;
SELECT 'LowCardinality(Nullable(String)), prefetch off';
SELECT lc, count() FROM t_two_level_few_keys GROUP BY lc ORDER BY lc SETTINGS enable_software_prefetch_in_aggregation = 0;

SELECT 'Nullable(String), prefetch on';
SELECT s, count() FROM t_two_level_few_keys GROUP BY s ORDER BY s SETTINGS enable_software_prefetch_in_aggregation = 1;
SELECT 'Nullable(String), prefetch off';
SELECT s, count() FROM t_two_level_few_keys GROUP BY s ORDER BY s SETTINGS enable_software_prefetch_in_aggregation = 0;

SELECT 'keys256, prefetch on';
SELECT a, b, c, count() FROM t_two_level_few_keys GROUP BY a, b, c ORDER BY a, b, c SETTINGS enable_software_prefetch_in_aggregation = 1;
SELECT 'keys256, prefetch off';
SELECT a, b, c, count() FROM t_two_level_few_keys GROUP BY a, b, c ORDER BY a, b, c SETTINGS enable_software_prefetch_in_aggregation = 0;

SELECT 'serialized, prefetch on';
SELECT str, b, count() FROM t_two_level_few_keys GROUP BY str, b ORDER BY str, b SETTINGS enable_software_prefetch_in_aggregation = 1;
SELECT 'serialized, prefetch off';
SELECT str, b, count() FROM t_two_level_few_keys GROUP BY str, b ORDER BY str, b SETTINGS enable_software_prefetch_in_aggregation = 0;

SELECT 'grown two-level, prefetch on and off';
SELECT count(), sum(cnt), sum(many * cnt) FROM (SELECT many, count() AS cnt FROM t_two_level_few_keys GROUP BY many) SETTINGS enable_software_prefetch_in_aggregation = 1;
SELECT count(), sum(cnt), sum(many * cnt) FROM (SELECT many, count() AS cnt FROM t_two_level_few_keys GROUP BY many) SETTINGS enable_software_prefetch_in_aggregation = 0;

DROP TABLE t_two_level_few_keys;
