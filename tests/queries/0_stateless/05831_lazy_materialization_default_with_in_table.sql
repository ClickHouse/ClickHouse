-- A column whose `DEFAULT` reads a set from a table with `IN`, itself or through the defaults of the columns it reads,
-- is not read lazily: an insert into the table between the main read and the lazy read changes the values the lazy read
-- fills. A set written in the default is constant, and such a column is read lazily.

SET enable_analyzer = 1;
SET optimize_read_in_order = 0;
SET optimize_move_to_prewhere = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS values_set;

CREATE TABLE values_set (k UInt64) ENGINE = Set;
INSERT INTO values_set VALUES (999);

CREATE TABLE l (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r (k UInt64, payload String) ENGINE = MergeTree ORDER BY k;
INSERT INTO l SELECT number FROM numbers(1000);
INSERT INTO r SELECT number, toString(number) FROM numbers(1000);

ALTER TABLE l ADD COLUMN n UInt8 DEFAULT k IN values_set;
ALTER TABLE l ADD COLUMN s String DEFAULT toString(n);
ALTER TABLE l ADD COLUMN c UInt8 DEFAULT k IN (998, 999);
ALTER TABLE l ADD COLUMN c1 UInt8 DEFAULT k IN (999);

SELECT 'order by';
SELECT k, n, s, c, c1 FROM l ORDER BY k DESC LIMIT 3;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1 SELECT k, n, s, c, c1 FROM l ORDER BY k DESC LIMIT 3)
WHERE explain LIKE '%Lazily read columns%';

SELECT 'join';
SELECT l.k, l.n, l.s, l.c, l.c1, r.payload FROM l JOIN r ON l.k = r.k ORDER BY l.k DESC LIMIT 3
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT l.k, l.n, l.s, l.c, l.c1, r.payload FROM l JOIN r ON l.k = r.k ORDER BY l.k DESC LIMIT 3
    SETTINGS query_plan_lazy_materialization_for_join = 1)
WHERE explain LIKE '%Lazily read columns%';

DROP TABLE l;
DROP TABLE r;
DROP TABLE values_set;
