-- Lazy materialization over joins: a column that a filter, a join condition or a join key uses below the LIMIT, and that
-- the result needs as well, is read again after the LIMIT, rather than carried through the joins - where a join is above
-- it and the lazily read columns of its table take more per row than the row index. Each query is compared with lazy
-- materialization off, and the plan shows the columns read lazily and whether a column crosses the LIMIT.

SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_lazy_materialization_for_join = 1;
SET query_plan_max_limit_for_lazy_materialization = 10;
-- Pinned so that both sides of a join are read directly, and the join keeps the order it is written in.
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET join_algorithm = 'hash';
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS j;
DROP TABLE IF EXISTS d;

CREATE TABLE l (k UInt64, a UInt64, s String, flag UInt8, heavy String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
CREATE TABLE r (k UInt64, b UInt64, rs String, rheavy String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
INSERT INTO l SELECT number, number % 100, toString(number % 7), number % 2, repeat('x', 100) FROM numbers(10000);
INSERT INTO r SELECT number, number % 50, toString(number % 5), repeat('y', 100) FROM numbers(10000);

SELECT '-- a filter in PREWHERE';
SELECT l.k, l.s, r.rheavy = repeat('y', 100) FROM l JOIN r ON l.k = r.k WHERE l.s = '3' ORDER BY l.a DESC, l.k LIMIT 3;
SELECT l.k, l.s, r.rheavy = repeat('y', 100) FROM l JOIN r ON l.k = r.k WHERE l.s = '3' ORDER BY l.a DESC, l.k LIMIT 3
SETTINGS query_plan_optimize_lazy_materialization = 0;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT l.k, l.s, r.rheavy = repeat('y', 100) FROM l JOIN r ON l.k = r.k WHERE l.s = '3' ORDER BY l.a DESC, l.k LIMIT 3)
WHERE explain LIKE '%Lazily read columns%';
SELECT countIf(explain LIKE '%__lazy_carried_%') FROM (EXPLAIN header = 1
    SELECT l.k, l.s, r.rheavy = repeat('y', 100) FROM l JOIN r ON l.k = r.k WHERE l.s = '3' ORDER BY l.a DESC, l.k LIMIT 3);

SELECT '-- a filter in WHERE';
SELECT l.k, l.s, r.rheavy = repeat('y', 100) FROM l JOIN r ON l.k = r.k WHERE l.s = '3' ORDER BY l.a DESC, l.k LIMIT 3
SETTINGS optimize_move_to_prewhere = 0;
SELECT l.k, l.s, r.rheavy = repeat('y', 100) FROM l JOIN r ON l.k = r.k WHERE l.s = '3' ORDER BY l.a DESC, l.k LIMIT 3
SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_lazy_materialization = 0;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT l.k, l.s, r.rheavy = repeat('y', 100) FROM l JOIN r ON l.k = r.k WHERE l.s = '3' ORDER BY l.a DESC, l.k LIMIT 3
    SETTINGS optimize_move_to_prewhere = 0)
WHERE explain LIKE '%Lazily read columns%';
SELECT countIf(explain LIKE '%__lazy_carried_%') FROM (EXPLAIN header = 1
    SELECT l.k, l.s, r.rheavy = repeat('y', 100) FROM l JOIN r ON l.k = r.k WHERE l.s = '3' ORDER BY l.a DESC, l.k LIMIT 3
    SETTINGS optimize_move_to_prewhere = 0);

SELECT '-- a JOIN ON condition of the build side';
SELECT l.k, r.rs, l.heavy = repeat('x', 100) FROM l JOIN r ON l.k = r.k AND r.rs = '3' ORDER BY l.a DESC, l.k LIMIT 3;
SELECT l.k, r.rs, l.heavy = repeat('x', 100) FROM l JOIN r ON l.k = r.k AND r.rs = '3' ORDER BY l.a DESC, l.k LIMIT 3
SETTINGS query_plan_optimize_lazy_materialization = 0;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT l.k, r.rs, l.heavy = repeat('x', 100) FROM l JOIN r ON l.k = r.k AND r.rs = '3' ORDER BY l.a DESC, l.k LIMIT 3)
WHERE explain LIKE '%Lazily read columns%';
SELECT countIf(explain LIKE '%__lazy_carried_%') FROM (EXPLAIN header = 1
    SELECT l.k, r.rs, l.heavy = repeat('x', 100) FROM l JOIN r ON l.k = r.k AND r.rs = '3' ORDER BY l.a DESC, l.k LIMIT 3);

SELECT '-- a join key';
SELECT l.k, r.k, r.rs, l.heavy = repeat('x', 100) FROM l JOIN r ON l.s = r.rs ORDER BY l.a DESC, l.k, r.k LIMIT 3;
SELECT l.k, r.k, r.rs, l.heavy = repeat('x', 100) FROM l JOIN r ON l.s = r.rs ORDER BY l.a DESC, l.k, r.k LIMIT 3
SETTINGS query_plan_optimize_lazy_materialization = 0;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT l.k, r.k, r.rs, l.heavy = repeat('x', 100) FROM l JOIN r ON l.s = r.rs ORDER BY l.a DESC, l.k, r.k LIMIT 3)
WHERE explain LIKE '%Lazily read columns%';
SELECT countIf(explain LIKE '%__lazy_carried_%') FROM (EXPLAIN header = 1
    SELECT l.k, r.k, r.rs, l.heavy = repeat('x', 100) FROM l JOIN r ON l.s = r.rs ORDER BY l.a DESC, l.k, r.k LIMIT 3);

SELECT '-- the padded side of a LEFT JOIN';
SELECT l.k, r.rs, l.heavy = repeat('x', 100) FROM l LEFT JOIN r ON l.k = r.k AND r.rs = '3' ORDER BY l.a DESC, l.k LIMIT 5;
SELECT l.k, r.rs, l.heavy = repeat('x', 100) FROM l LEFT JOIN r ON l.k = r.k AND r.rs = '3' ORDER BY l.a DESC, l.k LIMIT 5
SETTINGS query_plan_optimize_lazy_materialization = 0;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT l.k, r.rs, l.heavy = repeat('x', 100) FROM l LEFT JOIN r ON l.k = r.k AND r.rs = '3' ORDER BY l.a DESC, l.k LIMIT 5)
WHERE explain LIKE '%Lazily read columns%';

SELECT '-- a narrow column, the only one of its table read lazily, crosses the LIMIT';
-- `flag` takes 1 byte per row, less than the row index its table would need for a second read.
SELECT l.a, l.flag, r.rheavy = repeat('y', 100) FROM l JOIN r ON l.k = r.k WHERE l.flag = 1 ORDER BY l.a DESC, l.k LIMIT 3;
SELECT l.a, l.flag, r.rheavy = repeat('y', 100) FROM l JOIN r ON l.k = r.k WHERE l.flag = 1 ORDER BY l.a DESC, l.k LIMIT 3
SETTINGS query_plan_optimize_lazy_materialization = 0;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT l.a, l.flag, r.rheavy = repeat('y', 100) FROM l JOIN r ON l.k = r.k WHERE l.flag = 1 ORDER BY l.a DESC, l.k LIMIT 3)
WHERE explain LIKE '%Lazily read columns%';
SELECT countIf(explain LIKE '%__lazy_carried_%') > 0 FROM (EXPLAIN header = 1
    SELECT l.a, l.flag, r.rheavy = repeat('y', 100) FROM l JOIN r ON l.k = r.k WHERE l.flag = 1 ORDER BY l.a DESC, l.k LIMIT 3);

SELECT '-- a JSON path in PREWHERE and in a join key';
CREATE TABLE j (k UInt64, a UInt64, doc JSON) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
INSERT INTO j SELECT number, number % 100, toJSONString(map('status', if(number % 3 = 0, 'open', 'closed'), 'id', toString(number % 5))) FROM numbers(10000);
SELECT j.k, r.k, j.doc.status, j.doc.id, r.rheavy = repeat('y', 100) FROM j JOIN r ON CAST(j.doc.id, 'Nullable(String)') = r.rs
WHERE j.doc.status = 'open' ORDER BY j.a DESC, j.k, r.k LIMIT 3;
SELECT j.k, r.k, j.doc.status, j.doc.id, r.rheavy = repeat('y', 100) FROM j JOIN r ON CAST(j.doc.id, 'Nullable(String)') = r.rs
WHERE j.doc.status = 'open' ORDER BY j.a DESC, j.k, r.k LIMIT 3 SETTINGS query_plan_optimize_lazy_materialization = 0;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT j.k, r.k, j.doc.status, j.doc.id, r.rheavy = repeat('y', 100) FROM j JOIN r ON CAST(j.doc.id, 'Nullable(String)') = r.rs
    WHERE j.doc.status = 'open' ORDER BY j.a DESC, j.k, r.k LIMIT 3)
WHERE explain LIKE '%Lazily read columns%';
SELECT countIf(explain LIKE '%__lazy_carried_%') FROM (EXPLAIN header = 1
    SELECT j.k, r.k, j.doc.status, j.doc.id, r.rheavy = repeat('y', 100) FROM j JOIN r ON CAST(j.doc.id, 'Nullable(String)') = r.rs
    WHERE j.doc.status = 'open' ORDER BY j.a DESC, j.k, r.k LIMIT 3);

SELECT '-- a column some parts fill from a non-deterministic default crosses the LIMIT';
CREATE TABLE d (k UInt64, a UInt64, heavy String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64;
INSERT INTO d SELECT number, number % 100, repeat('x', 100) FROM numbers(10000);
ALTER TABLE d ADD COLUMN c String DEFAULT toString(rand());
-- The filter and the result see the same values of `c`.
SELECT count(), countIf(toUInt64(c) % 2 = 0) FROM (
    SELECT d.k, d.c AS c, d.heavy, r.rheavy FROM d JOIN r ON d.k = r.k WHERE toUInt64(d.c) % 2 = 0 ORDER BY d.a DESC, d.k LIMIT 5);
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT d.k, d.c, d.heavy, r.rheavy FROM d JOIN r ON d.k = r.k WHERE toUInt64(d.c) % 2 = 0 ORDER BY d.a DESC, d.k LIMIT 5)
WHERE explain LIKE '%Lazily read columns%';

DROP TABLE l;
DROP TABLE r;
DROP TABLE j;
DROP TABLE d;
