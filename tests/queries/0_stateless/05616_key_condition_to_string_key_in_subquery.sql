-- A subquery set of integers over the text of an integer key, or of dates over the text of a `Date` key, keeps the
-- primary key (#130315): that text holds one spelling per value, so the set's parse and the index agree.

DROP TABLE IF EXISTS t_u64;
DROP TABLE IF EXISTS t_alias;
DROP TABLE IF EXISTS t_u32;
DROP TABLE IF EXISTS t_i32;
DROP TABLE IF EXISTS t_i64;
DROP TABLE IF EXISTS t_null;
DROP TABLE IF EXISTS t_lc;
DROP TABLE IF EXISTS t_date;
DROP TABLE IF EXISTS t_u8;

CREATE TABLE t_u64 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
CREATE TABLE t_alias (k UInt64, s String ALIAS k) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
CREATE TABLE t_u32 (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
CREATE TABLE t_i32 (k Int32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
CREATE TABLE t_i64 (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
CREATE TABLE t_null (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4, allow_nullable_key = 1;
CREATE TABLE t_lc (k LowCardinality(UInt64)) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 4, allow_suspicious_low_cardinality_types = 1;
CREATE TABLE t_date (d Date) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 4;
CREATE TABLE t_u8 (k UInt8) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;

INSERT INTO t_u64 SELECT number * 7 FROM numbers(200);
INSERT INTO t_alias (k) SELECT number * 7 FROM numbers(200);
INSERT INTO t_u32 SELECT number * 7 FROM numbers(200);
INSERT INTO t_i32 SELECT number * 7 - 700 FROM numbers(200);
INSERT INTO t_i64 SELECT number * 7 - 700 FROM numbers(200);
INSERT INTO t_null SELECT if(number % 10 = 0, NULL, number * 7) FROM numbers(200);
INSERT INTO t_lc SELECT number * 7 FROM numbers(200);
INSERT INTO t_date SELECT toDate('2020-01-01') + number FROM numbers(200);
INSERT INTO t_u8 SELECT number + 1 FROM numbers(200);

SELECT 'the primary key is used';
SELECT 'u64', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_u64 WHERE toString(k) IN (SELECT toUInt64(707)))
    WHERE explain ILIKE '%Condition: (toString(k) in %';
SELECT 'alias', count() FROM (EXPLAIN indexes = 1 SELECT toString(k) AS k FROM t_u64 WHERE k IN (SELECT toUInt64(707)))
    WHERE explain ILIKE '%Condition: (toString(k) in %';
SELECT 'cast', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_u64 WHERE CAST(k AS String) IN (SELECT toUInt64(707)))
    WHERE explain ILIKE '%Condition: (CAST(k, \'String\') in %';
SELECT 'cast operator', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_u64 WHERE k::String IN (SELECT toUInt64(707)))
    WHERE explain ILIKE '%Condition: (CAST(k, \'String\') in %';
SELECT 'alias column', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_alias WHERE s IN (SELECT toUInt64(707)))
    WHERE explain ILIKE '%Condition: (_CAST(k, \'String\') in %';
SELECT 'u32', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_u32 WHERE toString(k) IN (SELECT toUInt64(707)))
    WHERE explain ILIKE '%Condition: (toString(k) in %';
SELECT 'i32', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_i32 WHERE toString(k) IN (SELECT toInt32(-343)))
    WHERE explain ILIKE '%Condition: (toString(k) in %';
SELECT 'nullable', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_null WHERE toString(k) IN (SELECT toUInt64(707)))
    WHERE explain ILIKE '%Condition: (toString(k) in %';
SELECT 'low cardinality', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lc WHERE toString(k) IN (SELECT toUInt64(707)))
    WHERE explain ILIKE '%Condition: (toString(k) in %';
SELECT 'date', count() FROM (EXPLAIN indexes = 1 SELECT count() FROM t_date WHERE toString(d) IN (SELECT toDate('2020-03-01')))
    WHERE explain ILIKE '%Condition: (toString(d) in %';

-- A full scan reads all 200 rows; with the key the read stays far below the limit.
SELECT 'the primary key prunes';
SELECT 'u64', k FROM t_u64 WHERE toString(k) IN (SELECT toUInt64(707))
    SETTINGS max_rows_to_read = 100, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0,
        enable_parallel_replicas = 0;
SELECT 'cast', k FROM t_u64 WHERE CAST(k AS String) IN (SELECT toUInt64(707))
    SETTINGS max_rows_to_read = 100, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0,
        enable_parallel_replicas = 0;
SELECT 'alias column', k FROM t_alias WHERE s IN (SELECT toUInt64(707))
    SETTINGS max_rows_to_read = 100, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0,
        enable_parallel_replicas = 0;
SELECT 'nullable', k FROM t_null WHERE toString(k) IN (SELECT toUInt64(707))
    SETTINGS max_rows_to_read = 100, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0,
        enable_parallel_replicas = 0;
SELECT 'low cardinality', k FROM t_lc WHERE toString(k) IN (SELECT toUInt64(707))
    SETTINGS max_rows_to_read = 100, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0,
        enable_parallel_replicas = 0;
SELECT 'date', d FROM t_date WHERE toString(d) IN (SELECT toDate('2020-03-01'))
    SETTINGS max_rows_to_read = 100, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0,
        enable_parallel_replicas = 0;

-- Each line: the rows the index-assisted `WHERE` returns, then the rows the predicate keeps on a full scan.
SELECT 'the index keeps every matching row';
SELECT 'u64', count(), (SELECT countIf(toString(k) IN (SELECT toUInt64(707))) FROM t_u64)
    FROM t_u64 WHERE toString(k) IN (SELECT toUInt64(707));
SELECT 'alias', count(), (SELECT countIf(k IN (SELECT toUInt64(707))) FROM (SELECT toString(k) AS k FROM t_u64))
    FROM (SELECT toString(k) AS k FROM t_u64 WHERE k IN (SELECT toUInt64(707)));
SELECT 'cast', count(), (SELECT countIf(CAST(k AS String) IN (SELECT toUInt64(707))) FROM t_u64)
    FROM t_u64 WHERE CAST(k AS String) IN (SELECT toUInt64(707));
SELECT 'alias column', count(), (SELECT countIf(s IN (SELECT toUInt64(707))) FROM t_alias)
    FROM t_alias WHERE s IN (SELECT toUInt64(707));
SELECT 'u32', count(), (SELECT countIf(toString(k) IN (SELECT arrayJoin([toUInt64(707), 4294967366]))) FROM t_u32)
    FROM t_u32 WHERE toString(k) IN (SELECT arrayJoin([toUInt64(707), 4294967366]));
SELECT 'i32', count(), (SELECT countIf(toString(k) IN (SELECT toInt32(-343))) FROM t_i32)
    FROM t_i32 WHERE toString(k) IN (SELECT toInt32(-343));
SELECT 'i64 key, u64 elements', count(), (SELECT countIf(toString(k) IN (SELECT arrayJoin([toUInt64(70), 18446744073709551609]))) FROM t_i64)
    FROM t_i64 WHERE toString(k) IN (SELECT arrayJoin([toUInt64(70), 18446744073709551609]));
SELECT 'u64 key, i8 elements', count(), (SELECT countIf(toString(k) IN (SELECT arrayJoin([toInt8(-7), toInt8(70)]))) FROM t_u64)
    FROM t_u64 WHERE toString(k) IN (SELECT arrayJoin([toInt8(-7), toInt8(70)]));
SELECT 'nullable', count(), (SELECT countIf(toString(k) IN (SELECT toUInt64(707))) FROM t_null)
    FROM t_null WHERE toString(k) IN (SELECT toUInt64(707));
SELECT 'nullable, transform_null_in', count(),
    (SELECT countIf(toString(k) IN (SELECT arrayJoin([NULL, toUInt64(707)]))) FROM t_null SETTINGS transform_null_in = 1)
    FROM t_null WHERE toString(k) IN (SELECT arrayJoin([NULL, toUInt64(707)])) SETTINGS transform_null_in = 1;
SELECT 'low cardinality', count(), (SELECT countIf(toString(k) IN (SELECT toUInt64(707))) FROM t_lc)
    FROM t_lc WHERE toString(k) IN (SELECT toUInt64(707));
SELECT 'date', count(), (SELECT countIf(toString(d) IN (SELECT toDate('2020-03-01'))) FROM t_date)
    FROM t_date WHERE toString(d) IN (SELECT toDate('2020-03-01'));
SELECT 'not in', count(), (SELECT countIf(toString(k) NOT IN (SELECT toUInt64(707))) FROM t_u64)
    FROM t_u64 WHERE toString(k) NOT IN (SELECT toUInt64(707));

-- A `Bool` renders as `true`, while the key's `'1'` parses to `true`, so such a set must not use the index.
SELECT 'a bool element';
SELECT 'bool', count(), (SELECT countIf(toString(k) IN (SELECT true)) FROM t_u8)
    FROM t_u8 WHERE toString(k) IN (SELECT true);
SELECT 'bool in a simple aggregate function', count(),
    (SELECT countIf(toString(k) IN (SELECT CAST(true, 'SimpleAggregateFunction(any, Bool)'))) FROM t_u8)
    FROM t_u8 WHERE toString(k) IN (SELECT CAST(true, 'SimpleAggregateFunction(any, Bool)'));

DROP TABLE t_u64;
DROP TABLE t_alias;
DROP TABLE t_u32;
DROP TABLE t_i32;
DROP TABLE t_i64;
DROP TABLE t_null;
DROP TABLE t_lc;
DROP TABLE t_date;
DROP TABLE t_u8;
