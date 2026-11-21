-- Random settings limits: use_statistics=(1, 1); materialize_statistics_on_insert=(1, 1); query_plan_optimize_join_order_limit=(10, None); query_plan_optimize_join_order_randomize=(0, 0); allow_reorder_prewhere_conditions=(1, 1); optimize_move_to_prewhere=(1, 1)
-- A comparison through a conversion that cannot change a value, e.g. `toFloat64(x) > 0` of a `Float64` `x`,
-- is estimated from the statistics of `x`, like `x > 0`, instead of by a default.

SET enable_parallel_replicas = 0;
SET min_marks_per_part_to_load_statistics = 0; -- the default 64 skips the statistics of this small part in PREWHERE

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS dim;

CREATE TABLE tab (k UInt32, x Float64, xn Nullable(Float64), t DateTime('UTC'), s String) ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = 'basic';
INSERT INTO tab SELECT number, 100 + number % 100, if(number % 4 = 0, NULL, 100 + number % 100),
    toDateTime('2026-01-01 00:00:00', 'UTC') + number, randomPrintableASCII(100) FROM numbers(20000);
CREATE TABLE dim (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO dim SELECT number FROM numbers(500);

SELECT '-- estimated rows of tab: the same as for the bare column, and the estimate';
WITH
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND toFloat64(x) > 0) WHERE explain LIKE '%⋈%' LIMIT 1) AS wrapped,
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND x > 0) WHERE explain LIKE '%⋈%' LIMIT 1) AS bare
SELECT wrapped = bare, wrapped;
WITH
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND toFloat64(x) > 150) WHERE explain LIKE '%⋈%' LIMIT 1) AS wrapped,
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND x > 150) WHERE explain LIKE '%⋈%' LIMIT 1) AS bare
SELECT wrapped = bare, wrapped;
WITH
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND toFloat64(xn) > 0) WHERE explain LIKE '%⋈%' LIMIT 1) AS wrapped,
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND xn > 0) WHERE explain LIKE '%⋈%' LIMIT 1) AS bare
SELECT wrapped = bare, wrapped;
WITH
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND toNullable(x) > 0) WHERE explain LIKE '%⋈%' LIMIT 1) AS wrapped,
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND x > 0) WHERE explain LIKE '%⋈%' LIMIT 1) AS bare
SELECT wrapped = bare, wrapped;
WITH
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND toDateTime(t) >= '2026-01-01 00:00:00') WHERE explain LIKE '%⋈%' LIMIT 1) AS wrapped,
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND t >= '2026-01-01 00:00:00') WHERE explain LIKE '%⋈%' LIMIT 1) AS bare
SELECT wrapped = bare, wrapped;
WITH
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND 0 < toFloat64(x)) WHERE explain LIKE '%⋈%' LIMIT 1) AS wrapped,
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND 0 < x) WHERE explain LIKE '%⋈%' LIMIT 1) AS bare
SELECT wrapped = bare, wrapped;
WITH
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND toFloat64(x) IN (100, 101)) WHERE explain LIKE '%⋈%' LIMIT 1) AS wrapped,
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND x IN (100, 101)) WHERE explain LIKE '%⋈%' LIMIT 1) AS bare
SELECT wrapped = bare, wrapped;
WITH
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND isNull(toFloat64(xn))) WHERE explain LIKE '%⋈%' LIMIT 1) AS wrapped,
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND isNull(xn)) WHERE explain LIKE '%⋈%' LIMIT 1) AS bare
SELECT wrapped = bare, wrapped;
SET statistics_max_set_size_for_exact_selectivity_estimation = 1;
WITH
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND toFloat64(x) IN (100, 101)) WHERE explain LIKE '%⋈%' LIMIT 1) AS wrapped,
    (SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
        SELECT count() FROM tab, dim WHERE tab.k = dim.k AND x IN (100, 101)) WHERE explain LIKE '%⋈%' LIMIT 1) AS bare
SELECT wrapped = bare, wrapped;
SET statistics_max_set_size_for_exact_selectivity_estimation = DEFAULT;

SELECT '-- a conversion that can change a value keeps the default';
SELECT extract(explain, 'tab\\[(\\d+)\\]') FROM (EXPLAIN actions = 1, compact = 1, pretty = 1
    SELECT count() FROM tab, dim WHERE tab.k = dim.k AND toFloat32(x) > 0) WHERE explain LIKE '%⋈%' LIMIT 1;

SELECT '-- PREWHERE runs the selective condition first: toFloat64(x) > 0 rejects no row';
SET explain_query_plan_default = 'legacy';
SELECT position(prewhere_line, 'equals(s') > 0
   AND position(prewhere_line, 'equals(s') < position(prewhere_line, 'greater(toFloat64(x)')
FROM (
    SELECT extractAll(replaceRegexpAll(explain, '__table1\.', ''), 'Prewhere filter column: ([^\n]+)')[1] AS prewhere_line FROM (
        EXPLAIN actions = 1 SELECT count() FROM tab WHERE toFloat64(x) > 0 AND s = 'absent'
    ) WHERE explain LIKE '%Prewhere filter column%'
);
SELECT count() FROM tab WHERE toFloat64(x) > 0 AND s = 'absent';

DROP TABLE tab;
DROP TABLE dim;
