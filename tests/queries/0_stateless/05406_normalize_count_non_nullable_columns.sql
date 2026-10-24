SET optimize_normalize_count_variants = 1;
SET optimize_functions_to_subcolumns = 0;
SET optimize_trivial_count_query = 0;
SET optimize_use_projections = 0;

DROP TABLE IF EXISTS count_non_nullable;
CREATE TABLE count_non_nullable
(
    k UInt64,
    i Int64,
    u UInt64,
    s String,
    lc LowCardinality(String),
    n Nullable(Int64),
    lcn LowCardinality(Nullable(String)),
    a Array(Nullable(Int64)),
    t Tuple(Nullable(Int64)),
    v Variant(Int64, String),
    d Dynamic
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO count_non_nullable VALUES
    (0, 1, 1, 'a', 'a', NULL, NULL, [NULL], tuple(NULL), NULL, NULL),
    (0, 2, 2, 'b', 'b', 2, 'b', [2], tuple(2), 2, 2),
    (1, 3, 3, 'c', 'c', NULL, 'c', [], tuple(NULL), 'c', 'c');

-- The eligible arguments disappear from the analyzed tree.
SELECT countIf(explain LIKE '%COLUMN id:%')
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count(i), count(u), count(s), count(lc), count(a), count(t) FROM count_non_nullable);

-- Canonical aggregate names share one state, and the unused argument is pruned.
SELECT countIf(endsWith(explain, 'Aggregates: count()'))
FROM (EXPLAIN PLAN actions = 1
    SELECT k, count(i) AS c FROM count_non_nullable GROUP BY k HAVING count(1) > 1);
SELECT countIf(endsWith(explain, 'Output: k, i'))
FROM (EXPLAIN PLAN actions = 1
    SELECT k, count(i) AS c FROM count_non_nullable GROUP BY k HAVING count(1) > 1);

-- Keep the original projection names and result types.
SELECT count(i), count(u), count(s), count(lc), count(a), count(t), toTypeName(count(i))
FROM count_non_nullable FORMAT TSVWithNames;
SELECT count(i), count(n), toTypeName(count(i)) FROM count_non_nullable WHERE 0;
SELECT k, count(i) AS c, count(), count(1)
FROM count_non_nullable GROUP BY k HAVING count(1) > 1 ORDER BY k;
WITH i AS value SELECT count(value) AS c FROM count_non_nullable;
SELECT count(i), sum(i), countIf(i > 1), countDistinct(i) FROM count_non_nullable;

-- A top-level `NULL` differs from a nested nullable element.
SELECT count(n), count(lcn), count(v), count(d) FROM count_non_nullable;
SELECT countIf(explain LIKE '%COLUMN id:%column_name: n,%') > 0,
       countIf(explain LIKE '%COLUMN id:%column_name: lcn,%') > 0,
       countIf(explain LIKE '%COLUMN id:%column_name: v,%') > 0,
       countIf(explain LIKE '%COLUMN id:%column_name: d,%') > 0
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT count(n), count(lcn), count(v), count(d) FROM count_non_nullable);

-- Other aggregates, combinators, and expressions keep their arguments.
SELECT countIf(explain LIKE '%COLUMN id:%column_name: i,%') > 0
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT sum(i), countIf(i > 0), countDistinct(i), countState(i), countOrNull(i), count(intDiv(i, i - i))
    FROM count_non_nullable);
SELECT count(intDiv(i, i - i)) FROM count_non_nullable; -- { serverError ILLEGAL_DIVISION }
SELECT count(i, u) FROM count_non_nullable; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- A plain column can still require expression evaluation.
ALTER TABLE count_non_nullable ADD COLUMN bad Int64 ALIAS intDiv(i, i - i);
ALTER TABLE count_non_nullable ADD COLUMN missing Int64 DEFAULT intDiv(i, i - i);
SELECT count(bad) FROM count_non_nullable; -- { serverError ILLEGAL_DIVISION }
SELECT k, count(missing) FROM count_non_nullable GROUP BY k; -- { serverError ILLEGAL_DIVISION }
SELECT count(bad) FROM (SELECT intDiv(i, i - i) AS bad FROM count_non_nullable); -- { serverError ILLEGAL_DIVISION }
DROP VIEW IF EXISTS count_non_nullable_view;
CREATE VIEW count_non_nullable_view AS SELECT intDiv(i, i - i) AS bad FROM count_non_nullable;
SELECT count(bad) FROM count_non_nullable_view; -- { serverError ILLEGAL_DIVISION }
DROP VIEW count_non_nullable_view;

-- A nullable outer-join input must retain its null map.
SELECT count(r.i), count() FROM numbers(3) AS l
LEFT JOIN count_non_nullable AS r ON l.number = r.k SETTINGS join_use_nulls = 1;

SET optimize_normalize_count_variants = 0;
SELECT countIf(explain LIKE '%COLUMN id:%column_name: i,%') > 0
FROM (EXPLAIN QUERY TREE run_passes = 1 SELECT count(i) FROM count_non_nullable);
SELECT count(i), sum(i) FROM count_non_nullable;
DROP TABLE count_non_nullable;

-- Stored columns can require throwing mutations or type conversions on read.
CREATE TABLE count_non_nullable_conversions (k UInt64, i Int64, s String)
ENGINE = MergeTree ORDER BY k;
INSERT INTO count_non_nullable_conversions VALUES (0, 1, 'bad');
ALTER TABLE count_non_nullable_conversions UPDATE i = intDiv(i, i - i) WHERE 1 SETTINGS mutations_sync = 0;
SET optimize_normalize_count_variants = 1;
SELECT countIf(explain LIKE '%COLUMN id:%column_name: i,%') > 0
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT k, count(i) FROM count_non_nullable_conversions GROUP BY k SETTINGS apply_mutations_on_fly = 1);
SELECT k, count(i) FROM count_non_nullable_conversions GROUP BY k SETTINGS apply_mutations_on_fly = 1; -- { serverError ILLEGAL_DIVISION }
ALTER TABLE count_non_nullable_conversions MODIFY COLUMN s Int64 SETTINGS alter_sync = 0;
SELECT countIf(explain LIKE '%COLUMN id:%column_name: s,%') > 0
FROM (EXPLAIN QUERY TREE run_passes = 1
    SELECT k, count(s) FROM count_non_nullable_conversions GROUP BY k SETTINGS apply_mutations_on_fly = 0);
SELECT k, count(s) FROM count_non_nullable_conversions GROUP BY k SETTINGS apply_mutations_on_fly = 0; -- { serverError CANNOT_PARSE_TEXT }
SELECT k, count(s) FROM count_non_nullable_conversions GROUP BY k SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0; -- { serverError CANNOT_PARSE_TEXT }
SELECT k, count(s) FROM count_non_nullable_conversions GROUP BY k SETTINGS apply_mutations_on_fly = 1; -- { serverError CANNOT_PARSE_TEXT }
SELECT k, count() FROM count_non_nullable_conversions GROUP BY k;
DROP TABLE count_non_nullable_conversions;
