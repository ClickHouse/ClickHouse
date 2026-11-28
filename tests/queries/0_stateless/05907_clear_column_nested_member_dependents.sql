-- CLEAR COLUMN of a Nested member: the member then reads defaults sized by the shared offsets of a stored sibling,
-- and the MATERIALIZED columns and projections rebuilt by the mutation must see the same value.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t1_compact;
DROP TABLE IF EXISTS t1_wide;
DROP TABLE IF EXISTS t2_compact;
DROP TABLE IF EXISTS t2_wide;
DROP TABLE IF EXISTS t3_compact;
DROP TABLE IF EXISTS t3_wide;
DROP TABLE IF EXISTS t4_compact;
DROP TABLE IF EXISTS t4_wide;
DROP TABLE IF EXISTS t5_both_compact;
DROP TABLE IF EXISTS t5_both_wide;
DROP TABLE IF EXISTS t5_unshared_compact;
DROP TABLE IF EXISTS t5_unshared_wide;
DROP TABLE IF EXISTS t6_compact;
DROP TABLE IF EXISTS t6_wide;
DROP TABLE IF EXISTS t7_compact;
DROP TABLE IF EXISTS t7_wide;
DROP TABLE IF EXISTS t8_compact;
DROP TABLE IF EXISTS t8_wide;
DROP TABLE IF EXISTS t9_compact;
DROP TABLE IF EXISTS t9_wide;
DROP TABLE IF EXISTS t10_compact;
DROP TABLE IF EXISTS t10_wide;
DROP TABLE IF EXISTS t11_compact;
DROP TABLE IF EXISTS t11_wide;
DROP TABLE IF EXISTS t12_compact;
DROP TABLE IF EXISTS t12_wide;
DROP TABLE IF EXISTS t13_compact;
DROP TABLE IF EXISTS t13_wide;
DROP TABLE IF EXISTS t14_compact;
DROP TABLE IF EXISTS t14_wide;
DROP TABLE IF EXISTS t15_compact;
DROP TABLE IF EXISTS t15_wide;

SELECT '-- 1. MATERIALIZED column over the cleared member';

CREATE TABLE t1_compact (k UInt32, n Nested(a UInt32, b UInt32), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t1_wide (k UInt32, n Nested(a UInt32, b UInt32), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t1_compact (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]), (2, [5], [6]), (3, [], []);
INSERT INTO t1_wide (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]), (2, [5], [6]), (3, [], []);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't1_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't1_wide' AND active;

ALTER TABLE t1_compact CLEAR COLUMN n.a;
ALTER TABLE t1_wide CLEAR COLUMN n.a;
SELECT k, n.a, n.b, m, m = arrayMap(x -> x + 1, n.a) FROM t1_compact ORDER BY k;
SELECT k, n.a, n.b, m, m = arrayMap(x -> x + 1, n.a) FROM t1_wide ORDER BY k;

OPTIMIZE TABLE t1_compact FINAL;
OPTIMIZE TABLE t1_wide FINAL;
SELECT k, n.a, n.b, m, m = arrayMap(x -> x + 1, n.a) FROM t1_compact ORDER BY k;
SELECT k, n.a, n.b, m, m = arrayMap(x -> x + 1, n.a) FROM t1_wide ORDER BY k;

SELECT '-- 2. projection over the cleared member';

CREATE TABLE t2_compact (k UInt32, n Nested(a UInt32, b UInt32), PROJECTION p (SELECT k, sum(length(n.a)), sum(arraySum(n.a)) GROUP BY k))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t2_wide (k UInt32, n Nested(a UInt32, b UInt32), PROJECTION p (SELECT k, sum(length(n.a)), sum(arraySum(n.a)) GROUP BY k))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t2_compact (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]), (2, [5], [6]), (3, [], []);
INSERT INTO t2_wide (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]), (2, [5], [6]), (3, [], []);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't2_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't2_wide' AND active;

ALTER TABLE t2_compact CLEAR COLUMN n.a;
ALTER TABLE t2_wide CLEAR COLUMN n.a;
SELECT 'projection', k, sum(length(n.a)), sum(arraySum(n.a)) FROM t2_compact GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT 'no projection', k, sum(length(n.a)), sum(arraySum(n.a)) FROM t2_compact GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;
SELECT 'projection', k, sum(length(n.a)), sum(arraySum(n.a)) FROM t2_wide GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;
SELECT 'no projection', k, sum(length(n.a)), sum(arraySum(n.a)) FROM t2_wide GROUP BY k ORDER BY k
SETTINGS optimize_use_projections = 0;

SELECT '-- 3. element types, and members declared as flat columns';

CREATE TABLE t3_compact
(
    k UInt32,
    n Nested(a Nullable(UInt32), b Array(UInt32), c LowCardinality(String), e Enum8('x' = 1, 'y' = 2), t Tuple(x Int32), d UInt8),
    `f.a` Array(UInt32),
    `f.b` Array(UInt32),
    m1 Array(Nullable(UInt32)) MATERIALIZED n.a,
    m2 Array(Array(UInt32)) MATERIALIZED n.b,
    m3 Array(String) MATERIALIZED arrayMap(x -> concat(x, '!'), n.c),
    m4 Array(String) MATERIALIZED arrayMap(v -> toString(v), n.e),
    m5 Array(Tuple(x Int32)) MATERIALIZED n.t,
    mf Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, `f.a`)
)
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t3_wide AS t3_compact
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t3_compact (k, n.a, n.b, n.c, n.e, n.t, n.d, `f.a`, `f.b`)
VALUES (1, [1, NULL], [[1], [2, 3]], ['p', 'q'], ['y', 'y'], [(5), (6)], [7, 8], [1, 2, 3], [4, 5, 6]);
INSERT INTO t3_wide (k, n.a, n.b, n.c, n.e, n.t, n.d, `f.a`, `f.b`)
VALUES (1, [1, NULL], [[1], [2, 3]], ['p', 'q'], ['y', 'y'], [(5), (6)], [7, 8], [1, 2, 3], [4, 5, 6]);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't3_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't3_wide' AND active;

ALTER TABLE t3_compact CLEAR COLUMN n.a, CLEAR COLUMN n.b, CLEAR COLUMN n.c, CLEAR COLUMN n.e, CLEAR COLUMN n.t, CLEAR COLUMN `f.a`;
ALTER TABLE t3_wide CLEAR COLUMN n.a, CLEAR COLUMN n.b, CLEAR COLUMN n.c, CLEAR COLUMN n.e, CLEAR COLUMN n.t, CLEAR COLUMN `f.a`;
SELECT k, n.a, n.b, n.c, n.e, n.t, n.d, `f.a`, `f.b` FROM t3_compact;
SELECT m1, m2, m3, m4, m5, mf FROM t3_compact;
SELECT
    m1 = n.a, m2 = n.b, m3 = arrayMap(x -> concat(x, '!'), n.c), m4 = arrayMap(v -> toString(v), n.e), m5 = n.t,
    mf = arrayMap(x -> x + 1, `f.a`)
FROM t3_compact;
SELECT k, n.a, n.b, n.c, n.e, n.t, n.d, `f.a`, `f.b` FROM t3_wide;
SELECT m1, m2, m3, m4, m5, mf FROM t3_wide;
SELECT
    m1 = n.a, m2 = n.b, m3 = arrayMap(x -> concat(x, '!'), n.c), m4 = arrayMap(v -> toString(v), n.e), m5 = n.t,
    mf = arrayMap(x -> x + 1, `f.a`)
FROM t3_wide;

SELECT '-- 4. one part stores the sibling, the other does not';

CREATE TABLE t4_compact (k UInt32, n Nested(a UInt32), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree PARTITION BY k ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t4_wide (k UInt32, n Nested(a UInt32), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree PARTITION BY k ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t4_compact (k, n.a) VALUES (1, [1, 2]);
INSERT INTO t4_wide (k, n.a) VALUES (1, [1, 2]);
ALTER TABLE t4_compact ADD COLUMN n.b Array(UInt32);
ALTER TABLE t4_wide ADD COLUMN n.b Array(UInt32);
INSERT INTO t4_compact (k, n.a, n.b) VALUES (2, [3], [4]);
INSERT INTO t4_wide (k, n.a, n.b) VALUES (2, [3], [4]);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't4_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't4_wide' AND active;

ALTER TABLE t4_compact CLEAR COLUMN n.a;
ALTER TABLE t4_wide CLEAR COLUMN n.a;
SELECT k, n.a, n.b, m FROM t4_compact ORDER BY k;
SELECT k, n.a, n.b, m FROM t4_wide ORDER BY k;

SELECT '-- 5. the cleared member reads as empty arrays: all members cleared, or offsets not shared';

CREATE TABLE t5_both_compact (k UInt32, n Nested(a UInt32, b UInt32), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t5_both_wide (k UInt32, n Nested(a UInt32, b UInt32), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t5_unshared_compact (k UInt32, n Nested(a UInt32, b UInt32), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 0, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t5_unshared_wide (k UInt32, n Nested(a UInt32, b UInt32), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t5_both_compact (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
INSERT INTO t5_both_wide (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
INSERT INTO t5_unshared_compact (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
INSERT INTO t5_unshared_wide (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't5_both_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't5_both_wide' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't5_unshared_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't5_unshared_wide' AND active;

ALTER TABLE t5_both_compact CLEAR COLUMN n.a, CLEAR COLUMN n.b;
ALTER TABLE t5_both_wide CLEAR COLUMN n.a, CLEAR COLUMN n.b;
ALTER TABLE t5_unshared_compact CLEAR COLUMN n.a;
ALTER TABLE t5_unshared_wide CLEAR COLUMN n.a;
SELECT k, n.a, n.b, m FROM t5_both_compact;
SELECT k, n.a, n.b, m FROM t5_both_wide;
SELECT k, n.a, n.b, m FROM t5_unshared_compact;
SELECT k, n.a, n.b, m FROM t5_unshared_wide;

SELECT '-- 6. a table column named like the sizes subcolumn of the sibling';

CREATE TABLE t6_compact (k UInt32, n Nested(a UInt32, b UInt32), `n.b.size0` UInt64, m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t6_wide (k UInt32, n Nested(a UInt32, b UInt32), `n.b.size0` UInt64, m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t6_compact (k, n.a, n.b, `n.b.size0`) VALUES (1, [1, 2], [3, 4], 7);
INSERT INTO t6_wide (k, n.a, n.b, `n.b.size0`) VALUES (1, [1, 2], [3, 4], 7);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't6_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't6_wide' AND active;

ALTER TABLE t6_compact CLEAR COLUMN n.a;
ALTER TABLE t6_wide CLEAR COLUMN n.a;
SELECT k, n.a, n.b, `n.b.size0`, m FROM t6_compact;
SELECT k, n.a, n.b, `n.b.size0`, m FROM t6_wide;

SELECT '-- 7. a MATERIALIZED member over the cleared one';

CREATE TABLE t7_compact
(
    k UInt32,
    `n.a` Array(UInt32),
    `n.c` Array(UInt32) MATERIALIZED arrayMap(x -> x * 2, `n.a`),
    m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, `n.a`)
)
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t7_wide AS t7_compact
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t7_compact (k, `n.a`) VALUES (1, [1, 2]);
INSERT INTO t7_wide (k, `n.a`) VALUES (1, [1, 2]);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't7_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't7_wide' AND active;

ALTER TABLE t7_compact CLEAR COLUMN `n.a`;
ALTER TABLE t7_wide CLEAR COLUMN `n.a`;
SELECT k, `n.a`, `n.c`, m FROM t7_compact;
SELECT k, `n.a`, `n.c`, m FROM t7_wide;

SELECT '-- 8. the sibling is converted by the same ALTER';

CREATE TABLE t8_compact (k UInt32, n Nested(a UInt32, b UInt32), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t8_wide (k UInt32, n Nested(a UInt32, b UInt32), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t8_compact (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
INSERT INTO t8_wide (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't8_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't8_wide' AND active;

ALTER TABLE t8_compact CLEAR COLUMN n.a, MODIFY COLUMN n.b Array(UInt64);
ALTER TABLE t8_wide CLEAR COLUMN n.a, MODIFY COLUMN n.b Array(UInt64);
SELECT k, n.a, n.b, toTypeName(n.b), m FROM t8_compact;
SELECT k, n.a, n.b, toTypeName(n.b), m FROM t8_wide;

SELECT '-- 9. a column TTL of another column that reads the cleared member';

CREATE TABLE t9_compact (k UInt32, n Nested(a UInt32, b UInt32), d DateTime TTL toDateTime('2100-01-01 00:00:00', 'UTC') + length(n.a), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t9_wide (k UInt32, n Nested(a UInt32, b UInt32), d DateTime TTL toDateTime('2100-01-01 00:00:00', 'UTC') + length(n.a), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t9_compact (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
INSERT INTO t9_wide (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't9_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't9_wide' AND active;

ALTER TABLE t9_compact CLEAR COLUMN n.a;
ALTER TABLE t9_wide CLEAR COLUMN n.a;
SELECT k, n.a, n.b, m, m = arrayMap(x -> x + 1, n.a) FROM t9_compact;
SELECT k, n.a, n.b, m, m = arrayMap(x -> x + 1, n.a) FROM t9_wide;

SELECT '-- 10. the sibling is resized by an UPDATE in the same ALTER';

CREATE TABLE t10_compact (k UInt32, n Nested(a UInt32, b UInt32), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t10_wide (k UInt32, n Nested(a UInt32, b UInt32), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t10_compact (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
INSERT INTO t10_wide (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't10_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't10_wide' AND active;

ALTER TABLE t10_compact UPDATE `n.a` = [7, 8, 9], `n.b` = [4, 5, 6] WHERE 1, CLEAR COLUMN n.a;
ALTER TABLE t10_wide UPDATE `n.a` = [7, 8, 9], `n.b` = [4, 5, 6] WHERE 1, CLEAR COLUMN n.a;
SELECT k, n.a, n.b, m, m = arrayMap(x -> x + 1, n.a) FROM t10_compact;
SELECT k, n.a, n.b, m, m = arrayMap(x -> x + 1, n.a) FROM t10_wide;

SELECT '-- 11. a scalar MATERIALIZED column with the prefix of the nested table';

CREATE TABLE t11_compact (k UInt32, n Nested(a UInt32, b UInt32), `n.c` UInt64 MATERIALIZED length(n.a), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t11_wide (k UInt32, n Nested(a UInt32, b UInt32), `n.c` UInt64 MATERIALIZED length(n.a), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t11_compact (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
INSERT INTO t11_wide (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't11_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't11_wide' AND active;

ALTER TABLE t11_compact CLEAR COLUMN n.a;
ALTER TABLE t11_wide CLEAR COLUMN n.a;
SELECT k, n.a, n.b, `n.c`, m, `n.c` = length(n.a), m = arrayMap(x -> x + 1, n.a) FROM t11_compact;
SELECT k, n.a, n.b, `n.c`, m, `n.c` = length(n.a), m = arrayMap(x -> x + 1, n.a) FROM t11_wide;

SELECT '-- 12. a column TTL of a scalar column with the prefix of the nested table';

CREATE TABLE t12_compact (k UInt32, n Nested(a UInt32, b UInt32), `n.d` DateTime TTL toDateTime('2100-01-01 00:00:00', 'UTC') + length(n.a), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t12_wide (k UInt32, n Nested(a UInt32, b UInt32), `n.d` DateTime TTL toDateTime('2100-01-01 00:00:00', 'UTC') + length(n.a), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t12_compact (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
INSERT INTO t12_wide (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't12_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't12_wide' AND active;

ALTER TABLE t12_compact CLEAR COLUMN n.a;
ALTER TABLE t12_wide CLEAR COLUMN n.a;
SELECT k, n.a, n.b, m, m = arrayMap(x -> x + 1, n.a) FROM t12_compact;
SELECT k, n.a, n.b, m, m = arrayMap(x -> x + 1, n.a) FROM t12_wide;

SELECT '-- 13. a bucketed Map MATERIALIZED column with the prefix of the nested table';

CREATE TABLE t13_compact (k UInt32, n Nested(a UInt32, b UInt32), `n.c` Map(String, UInt64) MATERIALIZED map('x', length(n.a)), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, serialization_info_version = 'with_types', map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets', min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t13_wide (k UInt32, n Nested(a UInt32, b UInt32), `n.c` Map(String, UInt64) MATERIALIZED map('x', length(n.a)), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, serialization_info_version = 'with_types', map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t13_compact (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
INSERT INTO t13_wide (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't13_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't13_wide' AND active;

ALTER TABLE t13_compact CLEAR COLUMN n.a;
ALTER TABLE t13_wide CLEAR COLUMN n.a;
SELECT k, n.a, n.b, `n.c`, m, `n.c`['x'] = length(n.a), m = arrayMap(x -> x + 1, n.a) FROM t13_compact;
SELECT k, n.a, n.b, `n.c`, m, `n.c`['x'] = length(n.a), m = arrayMap(x -> x + 1, n.a) FROM t13_wide;

SELECT '-- 14. a column TTL of a bucketed Map column with the prefix of the nested table';

CREATE TABLE t14_compact (k UInt32, n Nested(a UInt32, b UInt32), `n.e` Map(String, UInt64) TTL toDateTime('2100-01-01 00:00:00', 'UTC') + length(n.a), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, serialization_info_version = 'with_types', map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets', min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t14_wide (k UInt32, n Nested(a UInt32, b UInt32), `n.e` Map(String, UInt64) TTL toDateTime('2100-01-01 00:00:00', 'UTC') + length(n.a), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, n.a))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, serialization_info_version = 'with_types', map_serialization_version = 'with_buckets', map_serialization_version_for_zero_level_parts = 'with_buckets', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t14_compact (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
INSERT INTO t14_wide (k, n.a, n.b) VALUES (1, [1, 2], [3, 4]);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't14_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't14_wide' AND active;

ALTER TABLE t14_compact CLEAR COLUMN n.a;
ALTER TABLE t14_wide CLEAR COLUMN n.a;
SELECT k, n.a, n.b, m, m = arrayMap(x -> x + 1, n.a) FROM t14_compact;
SELECT k, n.a, n.b, m, m = arrayMap(x -> x + 1, n.a) FROM t14_wide;

SELECT '-- 15. a column TTL of the sibling';

CREATE TABLE t15_compact (k UInt32, `n.a` Array(UInt32), `n.b` Array(UInt32) TTL toDateTime('2100-01-01 00:00:00', 'UTC') + length(`n.a`), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, `n.a`))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE t15_wide (k UInt32, `n.a` Array(UInt32), `n.b` Array(UInt32) TTL toDateTime('2100-01-01 00:00:00', 'UTC') + length(`n.a`), m Array(UInt32) MATERIALIZED arrayMap(x -> x + 1, `n.a`))
ENGINE = MergeTree ORDER BY k
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t15_compact (k, `n.a`, `n.b`) VALUES (1, [1, 2], [3, 4]);
INSERT INTO t15_wide (k, `n.a`, `n.b`) VALUES (1, [1, 2], [3, 4]);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't15_compact' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't15_wide' AND active;

ALTER TABLE t15_compact CLEAR COLUMN `n.a`;
ALTER TABLE t15_wide CLEAR COLUMN `n.a`;
SELECT k, `n.a`, `n.b`, m, m = arrayMap(x -> x + 1, `n.a`) FROM t15_compact;
SELECT k, `n.a`, `n.b`, m, m = arrayMap(x -> x + 1, `n.a`) FROM t15_wide;

DROP TABLE t1_compact;
DROP TABLE t1_wide;
DROP TABLE t2_compact;
DROP TABLE t2_wide;
DROP TABLE t3_compact;
DROP TABLE t3_wide;
DROP TABLE t4_compact;
DROP TABLE t4_wide;
DROP TABLE t5_both_compact;
DROP TABLE t5_both_wide;
DROP TABLE t5_unshared_compact;
DROP TABLE t5_unshared_wide;
DROP TABLE t6_compact;
DROP TABLE t6_wide;
DROP TABLE t7_compact;
DROP TABLE t7_wide;
DROP TABLE t8_compact;
DROP TABLE t8_wide;
DROP TABLE t9_compact;
DROP TABLE t9_wide;
DROP TABLE t10_compact;
DROP TABLE t10_wide;
DROP TABLE t11_compact;
DROP TABLE t11_wide;
DROP TABLE t12_compact;
DROP TABLE t12_wide;
DROP TABLE t13_compact;
DROP TABLE t13_wide;
DROP TABLE t14_compact;
DROP TABLE t14_wide;
DROP TABLE t15_compact;
DROP TABLE t15_wide;
