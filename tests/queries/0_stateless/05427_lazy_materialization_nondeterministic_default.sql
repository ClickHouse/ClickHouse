-- For a part created before the columns were added, a read fills them from their `DEFAULT`. A column whose default
-- calls a non-deterministic function, itself or through the defaults of the columns it reads, is not read lazily:
-- the lazy read would fill it with values other than the ones the main read uses.

SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
INSERT INTO l SELECT number FROM numbers(1000);
INSERT INTO r SELECT number, toString(number) FROM numbers(1000);

ALTER TABLE l ADD COLUMN n UInt64 DEFAULT rand64();
ALTER TABLE l ADD COLUMN s String DEFAULT toString(n);
-- Through a column nothing reads.
ALTER TABLE l ADD COLUMN m UInt64 DEFAULT n + 1;
ALTER TABLE l ADD COLUMN t String DEFAULT toString(m);
-- A deterministic default, which the lazy read fills with the same values.
ALTER TABLE l ADD COLUMN d String DEFAULT toString(k * 2);

SELECT 'join';
SELECT count(), countIf(n = toUInt64(s)), countIf(n + 1 = toUInt64(t)), countIf(toString(k * 2) = d)
FROM (SELECT l.k AS k, l.n AS n, l.s AS s, l.t AS t, l.d AS d, r.payload FROM l JOIN r ON l.k = r.k WHERE l.n % 2 = 0 ORDER BY l.k LIMIT 20)
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT count(), countIf(n = toUInt64(s)), countIf(n + 1 = toUInt64(t)), countIf(toString(k * 2) = d)
FROM (SELECT l.k AS k, l.n AS n, l.s AS s, l.t AS t, l.d AS d, r.payload FROM l JOIN r ON l.k = r.k WHERE l.n % 2 = 0 ORDER BY l.k LIMIT 20)
SETTINGS query_plan_lazy_materialization_for_join = 0;

SELECT 'order by';
SELECT count(), countIf(n = toUInt64(s)), countIf(n + 1 = toUInt64(t)), countIf(toString(k * 2) = d)
FROM (SELECT k, n, s, t, d FROM l ORDER BY n LIMIT 20)
SETTINGS query_plan_optimize_lazy_materialization = 1;
SELECT count(), countIf(n = toUInt64(s)), countIf(n + 1 = toUInt64(t)), countIf(toString(k * 2) = d)
FROM (SELECT k, n, s, t, d FROM l ORDER BY n LIMIT 20)
SETTINGS query_plan_optimize_lazy_materialization = 0;

SELECT 'prewhere';
SELECT count(), countIf(n = toUInt64(s)), countIf(n + 1 = toUInt64(t)), countIf(toString(k * 2) = d)
FROM (SELECT k, n, s, t, d FROM l PREWHERE n % 2 = 0 ORDER BY k LIMIT 20)
SETTINGS query_plan_optimize_lazy_materialization = 1;

-- The deterministic default is still read lazily, the others are not.
SELECT 'lazily read';
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1 SELECT k, n, s, t, d FROM l ORDER BY n LIMIT 20 SETTINGS query_plan_optimize_lazy_materialization = 1)
WHERE explain LIKE '%Lazily read columns%';
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1 SELECT l.k, l.n, l.s, l.t, l.d, r.payload FROM l JOIN r ON l.k = r.k WHERE l.n % 2 = 0 ORDER BY l.k LIMIT 20
    SETTINGS query_plan_lazy_materialization_for_join = 1)
WHERE explain LIKE '%Lazily read columns%';

DROP TABLE l;
DROP TABLE r;
