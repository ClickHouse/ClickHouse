-- Set operations in `CHECK` constraints; the replicated `ALTER` case is in `05878_check_constraint_set_operations_replicated`.

-- Constraints declared in `CREATE`.
CREATE TABLE t_union_all (x UInt64, CONSTRAINT c CHECK x IN (SELECT 1 UNION ALL SELECT 2)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_union_all SETTINGS async_insert = 0 VALUES (1);
INSERT INTO t_union_all SETTINGS async_insert = 0 VALUES (3); -- { serverError VIOLATED_CONSTRAINT }

CREATE TABLE t_union_distinct (x UInt64, CONSTRAINT c CHECK x IN (SELECT 1 UNION DISTINCT SELECT 2)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_union_distinct SETTINGS async_insert = 0 VALUES (2);
INSERT INTO t_union_distinct SETTINGS async_insert = 0 VALUES (3); -- { serverError VIOLATED_CONSTRAINT }

CREATE TABLE t_intersect (x UInt64, CONSTRAINT c CHECK x IN (SELECT number FROM numbers(3) INTERSECT SELECT number FROM numbers(1, 5))) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_intersect SETTINGS async_insert = 0 VALUES (1);
INSERT INTO t_intersect SETTINGS async_insert = 0 VALUES (0); -- { serverError VIOLATED_CONSTRAINT }

CREATE TABLE t_except (x UInt64, CONSTRAINT c CHECK x IN (SELECT number FROM numbers(4) EXCEPT SELECT 3)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_except SETTINGS async_insert = 0 VALUES (0);
INSERT INTO t_except SETTINGS async_insert = 0 VALUES (3); -- { serverError VIOLATED_CONSTRAINT }

SELECT 'create', (SELECT count() FROM t_union_all), (SELECT count() FROM t_union_distinct), (SELECT count() FROM t_intersect), (SELECT count() FROM t_except);

-- Constraints added by `ALTER`.
CREATE TABLE t_alter_union_all (x UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t_alter_union_all ADD CONSTRAINT c CHECK x IN (SELECT 1 UNION ALL SELECT 2);
INSERT INTO t_alter_union_all SETTINGS async_insert = 0 VALUES (1);
INSERT INTO t_alter_union_all SETTINGS async_insert = 0 VALUES (3); -- { serverError VIOLATED_CONSTRAINT }

CREATE TABLE t_alter_union_distinct (x UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t_alter_union_distinct ADD CONSTRAINT c CHECK x IN (SELECT 1 UNION DISTINCT SELECT 2);
INSERT INTO t_alter_union_distinct SETTINGS async_insert = 0 VALUES (2);
INSERT INTO t_alter_union_distinct SETTINGS async_insert = 0 VALUES (3); -- { serverError VIOLATED_CONSTRAINT }

CREATE TABLE t_alter_intersect (x UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t_alter_intersect ADD CONSTRAINT c CHECK x IN (SELECT number FROM numbers(3) INTERSECT SELECT number FROM numbers(1, 5));
INSERT INTO t_alter_intersect SETTINGS async_insert = 0 VALUES (1);
INSERT INTO t_alter_intersect SETTINGS async_insert = 0 VALUES (0); -- { serverError VIOLATED_CONSTRAINT }

CREATE TABLE t_alter_except (x UInt64) ENGINE = MergeTree ORDER BY x;
ALTER TABLE t_alter_except ADD CONSTRAINT c CHECK x IN (SELECT number FROM numbers(4) EXCEPT SELECT 3);
INSERT INTO t_alter_except SETTINGS async_insert = 0 VALUES (0);
INSERT INTO t_alter_except SETTINGS async_insert = 0 VALUES (3); -- { serverError VIOLATED_CONSTRAINT }

SELECT 'alter', (SELECT count() FROM t_alter_union_all), (SELECT count() FROM t_alter_union_distinct), (SELECT count() FROM t_alter_intersect), (SELECT count() FROM t_alter_except);
