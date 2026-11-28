-- For a part created before a column was added, a read fills it from its `DEFAULT` or `MATERIALIZED` expression. A
-- column whose expression is non-deterministic, itself or through the expressions of the columns it reads, is not read
-- lazily: the lazy read would fill it with values other than the ones the main read uses.

SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS ag;
DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS i;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS values_set;

CREATE TABLE r (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
INSERT INTO r SELECT number, toString(number) FROM numbers(1000);

-- A non-deterministic function in the default.
CREATE TABLE l (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO l SELECT number FROM numbers(1000);
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

-- Non-deterministic through the aggregate function an argument names, or through a subcolumn.
CREATE TABLE ag (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO ag SELECT number FROM numbers(1000);
ALTER TABLE ag ADD COLUMN a UInt64 DEFAULT arrayReduce('groupArraySample(1)', range(1000))[1];
ALTER TABLE ag ADD COLUMN sa String DEFAULT toString(a);
ALTER TABLE ag ADD COLUMN n Tuple(v UInt64) DEFAULT tuple(rand64());
ALTER TABLE ag ADD COLUMN sn String DEFAULT toString(n.v);

SELECT 'join, aggregate function';
SELECT count(), countIf(a = toUInt64(sa))
FROM (SELECT ag.a AS a, ag.sa AS sa, r.payload FROM ag JOIN r ON ag.k = r.k WHERE ag.a % 2 = ag.k % 2 ORDER BY ag.k LIMIT 20)
SETTINGS query_plan_lazy_materialization_for_join = 1;

SELECT 'join, subcolumn';
SELECT count(), countIf(v = toUInt64(sn))
FROM (SELECT ag.n.v AS v, ag.sn AS sn, r.payload FROM ag JOIN r ON ag.k = r.k WHERE ag.n.v % 2 = 0 ORDER BY ag.k LIMIT 20)
SETTINGS query_plan_lazy_materialization_for_join = 1;

SELECT 'order by, aggregate function';
SELECT count(), countIf(a = toUInt64(sa)) FROM (SELECT k, a, sa FROM ag ORDER BY a, k LIMIT 20);

SELECT 'order by, subcolumn';
SELECT count(), countIf(v = toUInt64(sn)) FROM (SELECT k, n.v AS v, sn FROM ag ORDER BY n.v LIMIT 20);

-- `MATERIALIZED` columns are filled in the same way as `DEFAULT` ones.
CREATE TABLE m (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO m SELECT number FROM numbers(1000);
ALTER TABLE m ADD COLUMN n UInt64 MATERIALIZED rand64();
ALTER TABLE m ADD COLUMN s String MATERIALIZED toString(n);
ALTER TABLE m ADD COLUMN d String DEFAULT toString(n);

SELECT 'materialized, order by';
SELECT count(), countIf(n = toUInt64(s)), countIf(n = toUInt64(d)) FROM (SELECT k, n, s, d FROM m ORDER BY n LIMIT 20);
SELECT 'materialized, join';
SELECT count(), countIf(n = toUInt64(s)), countIf(n = toUInt64(d))
FROM (SELECT m.n AS n, m.s AS s, m.d AS d, r.payload FROM m JOIN r ON m.k = r.k WHERE m.n % 2 = 0 ORDER BY m.k LIMIT 20)
SETTINGS query_plan_lazy_materialization_for_join = 1;

-- The default is looked at with the settings of the features turned on, as the read fills it, so it works when the
-- query has the setting it was created with off.
CREATE TABLE f (k UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO f SELECT number FROM numbers(1000);
SET allow_fuzz_query_functions = 1;
ALTER TABLE f ADD COLUMN s String DEFAULT fuzzQuery('SELECT 1');
SET allow_fuzz_query_functions = 0;

SELECT 'disabled feature';
SELECT k, s != '' FROM f ORDER BY k DESC LIMIT 3;
SELECT f.k, f.s != '', r.payload FROM f JOIN r ON f.k = r.k ORDER BY f.k DESC LIMIT 3
SETTINGS query_plan_lazy_materialization_for_join = 1;

-- A set read from a table with `IN` can change between the main read and the lazy read. A set written in the default
-- is constant, and such a column is read lazily.
SET optimize_read_in_order = 0;
CREATE TABLE values_set (k UInt64) ENGINE = Set;
INSERT INTO values_set VALUES (999);
CREATE TABLE i (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO i SELECT number FROM numbers(1000);
ALTER TABLE i ADD COLUMN n UInt8 DEFAULT k IN values_set;
ALTER TABLE i ADD COLUMN s String DEFAULT toString(n);
ALTER TABLE i ADD COLUMN c UInt8 DEFAULT k IN (998, 999);
ALTER TABLE i ADD COLUMN c1 UInt8 DEFAULT k IN (999);

SELECT 'in table, order by';
SELECT k, n, s, c, c1 FROM i ORDER BY k DESC LIMIT 3;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1 SELECT k, n, s, c, c1 FROM i ORDER BY k DESC LIMIT 3)
WHERE explain LIKE '%Lazily read columns%';

SELECT 'in table, join';
SELECT i.k, i.n, i.s, i.c, i.c1, r.payload FROM i JOIN r ON i.k = r.k ORDER BY i.k DESC LIMIT 3
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT i.k, i.n, i.s, i.c, i.c1, r.payload FROM i JOIN r ON i.k = r.k ORDER BY i.k DESC LIMIT 3
    SETTINGS query_plan_lazy_materialization_for_join = 1)
WHERE explain LIKE '%Lazily read columns%';

DROP TABLE l;
DROP TABLE ag;
DROP TABLE m;
DROP TABLE f;
DROP TABLE i;
DROP TABLE r;
DROP TABLE values_set;
