-- SummingMergeTree and CoalescingMergeTree keep the alternative of Variant values in columns that are not summed.

SET optimize_on_insert = 1;

CREATE TABLE t_summing
(
    k UInt64,
    s UInt64,
    v Variant(Date, UInt64),
    a Array(Variant(Date, UInt64)),
    t Tuple(x String, y Variant(Date, UInt64)),
    m Map(String, Variant(Date, UInt64))
)
ENGINE = SummingMergeTree ORDER BY k;

-- Even keys hold a Date, odd keys a UInt64 that would also fit into a Date.
INSERT INTO t_summing SELECT k, 1, v, [v], ('x', v), map('x', v)
FROM (SELECT number AS k, if(number % 2 = 1, number::Variant(Date, UInt64), toDate(number)::Variant(Date, UInt64)) AS v FROM numbers(1000));

SELECT 'insert', variantType(v), variantType(a[1]), variantType(t.y), variantType(m['x']), count(), sum(s),
    countIf(toString(v) = if(k % 2 = 1, toString(k), toString(toDate(k))) AND toString(a[1]) = toString(v) AND toString(t.y) = toString(v) AND toString(m['x']) = toString(v))
FROM t_summing GROUP BY ALL ORDER BY ALL;

TRUNCATE TABLE t_summing;
SET optimize_on_insert = 0;

-- Two parts with the same keys; `s` differs so that a replicated table does not deduplicate the second insert.
INSERT INTO t_summing SELECT k, 1, v, [v], ('x', v), map('x', v)
FROM (SELECT number AS k, if(number % 2 = 1, number::Variant(Date, UInt64), toDate(number)::Variant(Date, UInt64)) AS v FROM numbers(1000));
INSERT INTO t_summing SELECT k, 2, v, [v], ('x', v), map('x', v)
FROM (SELECT number AS k, if(number % 2 = 1, number::Variant(Date, UInt64), toDate(number)::Variant(Date, UInt64)) AS v FROM numbers(1000));

SELECT 'final', variantType(v), variantType(a[1]), variantType(t.y), variantType(m['x']), count(), sum(s),
    countIf(toString(v) = if(k % 2 = 1, toString(k), toString(toDate(k))) AND toString(a[1]) = toString(v) AND toString(t.y) = toString(v) AND toString(m['x']) = toString(v))
FROM t_summing FINAL GROUP BY ALL ORDER BY ALL;

OPTIMIZE TABLE t_summing FINAL;

SELECT 'merge', variantType(v), variantType(a[1]), variantType(t.y), variantType(m['x']), count(), sum(s),
    countIf(toString(v) = if(k % 2 = 1, toString(k), toString(toDate(k))) AND toString(a[1]) = toString(v) AND toString(t.y) = toString(v) AND toString(m['x']) = toString(v))
FROM t_summing GROUP BY ALL ORDER BY ALL;

-- `v` is not in the `columns` list, so it is not coalesced.
CREATE TABLE t_coalescing (k UInt64, s Nullable(UInt64), v Variant(Date, UInt64)) ENGINE = CoalescingMergeTree(s) ORDER BY k;
INSERT INTO t_coalescing SELECT number, 1, if(number % 2 = 1, number::Variant(Date, UInt64), toDate(number)::Variant(Date, UInt64)) FROM numbers(1000);
INSERT INTO t_coalescing SELECT number, 2, if(number % 2 = 1, number::Variant(Date, UInt64), toDate(number)::Variant(Date, UInt64)) FROM numbers(1000);

SELECT 'coalescing', variantType(v), count(), sum(s), countIf(toString(v) = if(k % 2 = 1, toString(k), toString(toDate(k))))
FROM t_coalescing FINAL GROUP BY ALL ORDER BY ALL;

-- A nested map with a composite key is merged by `mergeMap`; its `Float64` values are kept as a row of the source column.
CREATE TABLE t_map (k UInt64, FirstMap Nested(k1 UInt32, k2ID Int8, s Float64)) ENGINE = SummingMergeTree ORDER BY k;
INSERT INTO t_map VALUES (1, [1], [1], [100]), (2, [1], [1], [1]);
INSERT INTO t_map VALUES (0, [1], [1], [1000]), (2, [1], [1], [2]), (2, [2], [1], [4]);
SELECT 'map', k, FirstMap.k1, FirstMap.k2ID, FirstMap.s FROM t_map FINAL ORDER BY k;

DROP TABLE t_summing;
DROP TABLE t_coalescing;
DROP TABLE t_map;
