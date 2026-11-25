-- The statistics-based choice of the IEJoin key conditions over a join input filtered on other columns.
-- A predicate that does not read an operand leaves its value range representative (`FilterOnOtherColumns`),
-- under the assumption that the operand is independent of the columns the predicate reads, so the conditions are still
-- chosen by selectivity. A predicate that reads an operand, or a row drop that is not attributed to the columns
-- a predicate reads, makes the choice fall back to the first two conditions in syntax order.
-- The conditions chosen by selectivity are `a1 < b1 AND a3 < b3`; the syntax order is `a1 < b1 AND a2 < b2`.

SET join_algorithm = 'ie_join';
SET join_use_nulls = 0;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
-- The printed conditions are mirrored when the join order optimizer swaps the sides
-- (e.g. under randomized `query_plan_optimize_join_order_randomize`); disable it to keep them stable.
SET query_plan_optimize_join_order_limit = 0;

DROP TABLE IF EXISTS t_foc_l;
DROP TABLE IF EXISTS t_foc_r;
DROP TABLE IF EXISTS t_foc_d;

CREATE TABLE t_foc_l (c UInt32, a1 UInt32, a2 UInt32, a3 UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

-- `f` is not an operand of the join conditions.
CREATE TABLE t_foc_r (f UInt32, b1 UInt32, b2 UInt32, b3 UInt32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

-- `t_foc_r` with columns computed from `b3` and `f`.
CREATE TABLE t_foc_d (
    f UInt32, b1 UInt32, b2 UInt32, b3 UInt32,
    m UInt32 MATERIALIZED b3, d UInt32 DEFAULT b3, mf UInt32 MATERIALIZED f)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

INSERT INTO t_foc_l SELECT 0, number % 1000, number % 1000, (number * 97) % 100000 FROM numbers(1000);
INSERT INTO t_foc_r SELECT number % 10, number % 1000, 1000 + number % 1000, number % 1000 FROM numbers(1000);
INSERT INTO t_foc_d (f, b1, b2, b3) SELECT number % 10, number % 1000, 1000 + number % 1000, number % 1000 FROM numbers(1000);

SELECT '-- unfiltered input: by selectivity';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN t_foc_r AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

-- Pin both PREWHERE optimizer modes because the stateless-test randomizer changes this setting: the filter is
-- either a `FilterStep` right above the read or a PREWHERE inside it.
SELECT '-- WHERE on a column that is not an operand, PREWHERE optimization disabled: by selectivity';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT * FROM t_foc_r WHERE f < 5) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
    SETTINGS optimize_move_to_prewhere = 0
) WHERE explain LIKE '%Conditions:%';

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_foc_l AS l JOIN (SELECT * FROM t_foc_r WHERE f < 5) AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS optimize_move_to_prewhere = 0;

SELECT '-- WHERE on a column that is not an operand, PREWHERE optimization enabled: by selectivity';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT * FROM t_foc_r WHERE f < 5) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
    SETTINGS optimize_move_to_prewhere = 1
) WHERE explain LIKE '%Conditions:%';

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_foc_l AS l JOIN (SELECT * FROM t_foc_r WHERE f < 5) AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS optimize_move_to_prewhere = 1;

-- The oracle: the same predicate as a filter over CROSS JOIN.
SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_foc_l AS l, (SELECT * FROM t_foc_r WHERE f < 5) AS r
WHERE l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS join_algorithm = 'hash';

SELECT '-- explicit PREWHERE on a column that is not an operand: by selectivity';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT * FROM t_foc_r PREWHERE f < 5) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT '-- both inputs filtered on columns that are not operands: by selectivity';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM (SELECT * FROM t_foc_l WHERE c = 0) AS l JOIN (SELECT * FROM t_foc_r WHERE f < 5) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT '-- WHERE on an operand: syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT * FROM t_foc_r WHERE f < 5 AND b3 < 500) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT '-- a predicate that reads an operand through an expression reads the operand: syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT * FROM t_foc_r WHERE f < 5 AND b3 + 1 > 1) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_foc_l AS l JOIN (SELECT * FROM t_foc_r WHERE f < 5 AND b3 + 1 > 1) AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3;

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_foc_l AS l, (SELECT * FROM t_foc_r WHERE f < 5 AND b3 + 1 > 1) AS r
WHERE l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS join_algorithm = 'hash';

-- A filter on a column computed below it reads whatever the column is computed from. Pushed into the read, the
-- predicate reads `b3`; kept above the expression, it reads only `x`, which the statistics of the read do not
-- describe, so it drops rows of every column.
SELECT '-- a filter on a column computed from an operand: syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT * FROM (SELECT f, b1, b2, b3, b3 + 1 AS x FROM t_foc_r) WHERE x < 500) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT '-- the same filter not pushed down: syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT * FROM (SELECT f, b1, b2, b3, b3 + 1 AS x FROM t_foc_r) WHERE x < 500) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
    SETTINGS query_plan_filter_push_down = 0, query_plan_merge_expressions = 0, optimize_move_to_prewhere = 0
) WHERE explain LIKE '%Conditions:%';

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_foc_l AS l
JOIN (SELECT * FROM (SELECT f, b1, b2, b3, b3 + 1 AS x FROM t_foc_r) WHERE x < 500) AS r
ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS query_plan_filter_push_down = 0, query_plan_merge_expressions = 0, optimize_move_to_prewhere = 0;

SELECT count(), sum(a1 + a3 + b1 + b3) FROM t_foc_l AS l,
    (SELECT * FROM (SELECT f, b1, b2, b3, b3 + 1 AS x FROM t_foc_r) WHERE x < 500) AS r
WHERE l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
SETTINGS join_algorithm = 'hash';

SELECT '-- the same filter on a computed column named like a stored column that is not an operand: syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT * FROM (SELECT b1, b2, b3, b3 + 1 AS f FROM t_foc_r) WHERE f < 500) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
    SETTINGS query_plan_filter_push_down = 0, query_plan_merge_expressions = 0, optimize_move_to_prewhere = 0
) WHERE explain LIKE '%Conditions:%';

-- A predicate on a column computed by a `DEFAULT` or `MATERIALIZED` expression reads the columns the expression reads,
-- and a predicate on those reads the computed column too.
SELECT '-- WHERE on a MATERIALIZED column computed from an operand: syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT b1, b2, b3 FROM t_foc_d WHERE m < 500) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT '-- WHERE on a DEFAULT column computed from an operand: syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT b1, b2, b3 FROM t_foc_d WHERE d < 500) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT '-- WHERE on the column that a MATERIALIZED operand is computed from: syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT b1, b2, m FROM t_foc_d WHERE b3 < 500) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.m
) WHERE explain LIKE '%Conditions:%';

SELECT '-- WHERE on a MATERIALIZED column computed from a column that is not an operand: by selectivity';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT b1, b2, b3 FROM t_foc_d WHERE mf < 5) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT '-- a predicate that selects rows by their position: syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT * FROM t_foc_r WHERE rowNumberInAllBlocks() < 500) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

-- Rows dropped by something other than a predicate are not attributed to columns.
SELECT '-- a LIMIT over the filtered input: syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT * FROM t_foc_r WHERE f < 5 LIMIT 1000) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
) WHERE explain LIKE '%Conditions:%';

SELECT '-- a non-throwing read limit on the filtered input: syntax order';
SELECT extract(explain, 'Conditions: .*') FROM (
    EXPLAIN actions = 1
    SELECT count() FROM t_foc_l AS l JOIN (SELECT * FROM t_foc_r WHERE f < 5) AS r
    ON l.a1 < r.b1 AND l.a2 < r.b2 AND l.a3 < r.b3
    SETTINGS max_rows_to_read = 1000000, read_overflow_mode = 'break'
) WHERE explain LIKE '%Conditions:%';

DROP TABLE t_foc_l;
DROP TABLE t_foc_r;
DROP TABLE t_foc_d;
