-- A column is not read lazily when its `DEFAULT` is non-deterministic through the aggregate function an argument names,
-- or through a subcolumn of a column with a non-deterministic default. For a part created before the columns were
-- added, the lazy read would fill it with values other than the ones the main read uses.

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

ALTER TABLE l ADD COLUMN a UInt64 DEFAULT arrayReduce('groupArraySample(1)', range(1000))[1];
ALTER TABLE l ADD COLUMN sa String DEFAULT toString(a);
ALTER TABLE l ADD COLUMN n Tuple(v UInt64) DEFAULT tuple(rand64());
ALTER TABLE l ADD COLUMN sn String DEFAULT toString(n.v);

SELECT 'join, aggregate function';
SELECT count(), countIf(a = toUInt64(sa))
FROM (SELECT l.a AS a, l.sa AS sa, r.payload FROM l JOIN r ON l.k = r.k WHERE l.a % 2 = l.k % 2 ORDER BY l.k LIMIT 20)
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT count(), countIf(a = toUInt64(sa))
FROM (SELECT l.a AS a, l.sa AS sa, r.payload FROM l JOIN r ON l.k = r.k WHERE l.a % 2 = l.k % 2 ORDER BY l.k LIMIT 20)
SETTINGS query_plan_lazy_materialization_for_join = 0;

SELECT 'join, subcolumn';
SELECT count(), countIf(v = toUInt64(sn))
FROM (SELECT l.n.v AS v, l.sn AS sn, r.payload FROM l JOIN r ON l.k = r.k WHERE l.n.v % 2 = 0 ORDER BY l.k LIMIT 20)
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT count(), countIf(v = toUInt64(sn))
FROM (SELECT l.n.v AS v, l.sn AS sn, r.payload FROM l JOIN r ON l.k = r.k WHERE l.n.v % 2 = 0 ORDER BY l.k LIMIT 20)
SETTINGS query_plan_lazy_materialization_for_join = 0;

SELECT 'order by, aggregate function';
SELECT count(), countIf(a = toUInt64(sa)) FROM (SELECT k, a, sa FROM l ORDER BY a, k LIMIT 20)
SETTINGS query_plan_optimize_lazy_materialization = 1;
SELECT count(), countIf(a = toUInt64(sa)) FROM (SELECT k, a, sa FROM l ORDER BY a, k LIMIT 20)
SETTINGS query_plan_optimize_lazy_materialization = 0;

SELECT 'order by, subcolumn';
SELECT count(), countIf(v = toUInt64(sn)) FROM (SELECT k, n.v AS v, sn FROM l ORDER BY n.v LIMIT 20)
SETTINGS query_plan_optimize_lazy_materialization = 1;
SELECT count(), countIf(v = toUInt64(sn)) FROM (SELECT k, n.v AS v, sn FROM l ORDER BY n.v LIMIT 20)
SETTINGS query_plan_optimize_lazy_materialization = 0;

DROP TABLE l;
DROP TABLE r;
