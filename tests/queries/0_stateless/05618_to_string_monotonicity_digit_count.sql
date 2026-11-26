-- `toString` of an integer keeps the order only between integers with the same number of digits. Each granule (and
-- each index granule of `t_skip`) here spans a number of nines and the next power of ten, whose texts are in the opposite order.

DROP TABLE IF EXISTS t_u64;
DROP TABLE IF EXISTS t_i64;
DROP TABLE IF EXISTS t_skip;
DROP TABLE IF EXISTS t_prune;
DROP TABLE IF EXISTS t_prune_i64;
DROP TABLE IF EXISTS t_bool;

CREATE TABLE t_u64 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
CREATE TABLE t_i64 (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
CREATE TABLE t_skip (k UInt64, INDEX i k TYPE minmax GRANULARITY 2) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS index_granularity = 1;
CREATE TABLE t_prune (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
CREATE TABLE t_prune_i64 (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
CREATE TABLE t_bool (b Bool) ENGINE = MergeTree ORDER BY b SETTINGS index_granularity = 1;

INSERT INTO t_u64 VALUES (999999999999999), (1000000000000000), (999999999999999999), (1000000000000000000),
    (9999999999999999999), (10000000000000000000);
INSERT INTO t_i64 VALUES (999999999999999), (1000000000000000), (999999999999999999), (1000000000000000000);
INSERT INTO t_skip VALUES (999999999999999), (1000000000000000), (999999999999999999), (1000000000000000000),
    (9999999999999999999), (10000000000000000000);
INSERT INTO t_prune SELECT 1000000000000000000 + number * 7 FROM numbers(200);
INSERT INTO t_prune_i64 SELECT 1000000000000000000 + number * 7 FROM numbers(200);
INSERT INTO t_bool VALUES (false), (true);

SET use_statistics_for_part_pruning = 0;

-- Each line: the rows the index-assisted `WHERE` returns, then the rows the predicate keeps on a full scan.
SELECT 'u64 =', count(), (SELECT countIf(toString(k) = '999999999999999999') FROM t_u64)
    FROM t_u64 WHERE toString(k) = '999999999999999999';
SELECT 'u64 in', count(),
    (SELECT countIf(toString(k) IN ('999999999999999', '999999999999999999', '9999999999999999999')) FROM t_u64)
    FROM t_u64 WHERE toString(k) IN ('999999999999999', '999999999999999999', '9999999999999999999');
SELECT 'u64 in subquery', count(),
    (SELECT countIf(toString(k) IN (SELECT arrayJoin([999999999999999, 999999999999999999, 9999999999999999999]))) FROM t_u64)
    FROM t_u64 WHERE toString(k) IN (SELECT arrayJoin([999999999999999, 999999999999999999, 9999999999999999999]));
SELECT 'u64 not in subquery', count(),
    (SELECT countIf(toString(k) NOT IN (SELECT arrayJoin([999999999999999, 999999999999999999, 9999999999999999999]))) FROM t_u64)
    FROM t_u64 WHERE toString(k) NOT IN (SELECT arrayJoin([999999999999999, 999999999999999999, 9999999999999999999]));
SELECT 'cast in subquery', count(), (SELECT countIf(CAST(k AS String) IN (SELECT toUInt64(999999999999999999))) FROM t_u64)
    FROM t_u64 WHERE CAST(k AS String) IN (SELECT toUInt64(999999999999999999));
SELECT 'i64 =', count(), (SELECT countIf(toString(k) = '999999999999999999') FROM t_i64)
    FROM t_i64 WHERE toString(k) = '999999999999999999';
SELECT 'i64 in subquery', count(),
    (SELECT countIf(toString(k) IN (SELECT arrayJoin([toInt64(999999999999999), 999999999999999999]))) FROM t_i64)
    FROM t_i64 WHERE toString(k) IN (SELECT arrayJoin([toInt64(999999999999999), 999999999999999999]));
SELECT 'skip index =', count(), (SELECT countIf(toString(k) = '999999999999999999') FROM t_skip)
    FROM t_skip WHERE toString(k) = '999999999999999999';
SELECT 'skip index in subquery', count(),
    (SELECT countIf(toString(k) IN (SELECT arrayJoin([999999999999999, 999999999999999999, 9999999999999999999]))) FROM t_skip)
    FROM t_skip WHERE toString(k) IN (SELECT arrayJoin([999999999999999, 999999999999999999, 9999999999999999999]));

-- Integers with the same number of digits keep the order, so the key still prunes: a full scan reads all 200 rows.
SELECT 'prunes =', k FROM t_prune WHERE toString(k) = '1000000000000000707'
    SETTINGS max_rows_to_read = 100, use_query_condition_cache = 0, enable_parallel_replicas = 0;
SELECT 'prunes in subquery', k FROM t_prune WHERE toString(k) IN (SELECT toUInt64(1000000000000000707))
    SETTINGS max_rows_to_read = 100, use_query_condition_cache = 0, enable_parallel_replicas = 0;
SELECT 'prunes i64 =', k FROM t_prune_i64 WHERE toString(k) = '1000000000000000707'
    SETTINGS max_rows_to_read = 100, use_query_condition_cache = 0, enable_parallel_replicas = 0;

-- A `Bool` renders 0 and 1 as configurable strings, here in the opposite order.
SET bool_true_representation = 'a', bool_false_representation = 'z';
SELECT 'bool =', count(), (SELECT countIf(toString(b) = 'z') FROM t_bool) FROM t_bool WHERE toString(b) = 'z';

DROP TABLE t_u64;
DROP TABLE t_i64;
DROP TABLE t_skip;
DROP TABLE t_prune;
DROP TABLE t_prune_i64;
DROP TABLE t_bool;
