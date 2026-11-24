-- A selective filter or a point lookup leaves blocks with a few rows and the whole part dictionary:
-- functions over the LowCardinality column must give the same values as over the plain column.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_lc_used_keys;
CREATE TABLE t_lc_used_keys (id UInt64, s LowCardinality(String), ns LowCardinality(Nullable(String)), n LowCardinality(UInt32))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lc_used_keys SELECT number, concat('v', toString(number % 1000)),
    if(number % 7 = 0, NULL, concat('v', toString(number % 1000))), number % 1000 FROM numbers(10000);

SELECT id, upper(s), concat(s, '!'), s = 'v43', substring(s, 2), upper(ns), ns = 'v43', isNull(ns), n + 1
FROM t_lc_used_keys WHERE id IN (0, 43, 4243, 9999) ORDER BY id;

SELECT id, modulo(1000, n) FROM t_lc_used_keys WHERE id IN (43, 4243, 9999) ORDER BY id;

SELECT count(),
    countIf(lower(upper(s)) != lower(upper(CAST(s AS String)))),
    countIf(ifNull(upper(ns), '<null>') != ifNull(upper(CAST(ns AS Nullable(String))), '<null>'))
FROM t_lc_used_keys WHERE id % 997 = 3;

SELECT countIf(upper(s) IN ('V3', 'V0', 'V997')), countIf(upper(CAST(s AS String)) IN ('V3', 'V0', 'V997'))
FROM t_lc_used_keys WHERE id % 997 = 3 AND upper(s) != ''
SETTINGS optimize_move_to_prewhere = 1, move_all_conditions_to_prewhere = 1, enable_multiple_prewhere_read_steps = 1,
    allow_reorder_prewhere_conditions = 0;

SELECT count(),
    countIf(lower(upper(s)) != lower(upper(CAST(s AS String)))),
    countIf(ifNull(upper(ns), '<null>') != ifNull(upper(CAST(ns AS Nullable(String))), '<null>'))
FROM t_lc_used_keys SETTINGS max_block_size = 65536;

DROP TABLE t_lc_used_keys;

-- A point lookup must not run the function on a value that only another row holds: here it would throw.
DROP TABLE IF EXISTS t_lc_unused_key;
CREATE TABLE t_lc_unused_key (id UInt64, s LowCardinality(String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lc_unused_key SELECT number, if(number = 5000, 'longer than FixedString(5)', concat('v', toString(number % 1000)))
FROM numbers(10000);

SELECT id, toFixedString(s, 5) FROM t_lc_unused_key WHERE id = 43;

DROP TABLE t_lc_unused_key;

-- `length` of a FixedString and `IN` an empty set do not depend on the values; with Nullable they do, unless `transform_null_in = 1`.
DROP TABLE IF EXISTS t_lc_fixed;
CREATE TABLE t_lc_fixed (id UInt64, f LowCardinality(FixedString(3)), nf LowCardinality(Nullable(FixedString(3))))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lc_fixed SELECT number, toString(number % 1000), if(number % 7 = 0, NULL, toString(number % 1000)) FROM numbers(10000);

SELECT id, f, length(f), length(nf) FROM t_lc_fixed WHERE id IN (0, 43, 4243, 9999) ORDER BY id;
SELECT id, f IN (SELECT f FROM t_lc_fixed WHERE id > 100000), nf NOT IN (SELECT nf FROM t_lc_fixed WHERE id > 100000)
FROM t_lc_fixed WHERE id IN (0, 43, 4243, 9999) ORDER BY id;
SELECT id, nf IN (SELECT nf FROM t_lc_fixed WHERE id > 100000), nf NOT IN (SELECT nf FROM t_lc_fixed WHERE id > 100000)
FROM t_lc_fixed WHERE id IN (0, 43, 4243, 9999) ORDER BY id SETTINGS transform_null_in = 1;

DROP TABLE t_lc_fixed;
