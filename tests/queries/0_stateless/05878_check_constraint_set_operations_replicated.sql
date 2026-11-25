-- Tags: zookeeper

-- A replica applies an `ALTER` with the constraint re-parsed from the metadata in Keeper, whose set operations are not normalized.

CREATE TABLE t_union_all (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05878_t_union_all', 'r1') ORDER BY x;
ALTER TABLE t_union_all ADD CONSTRAINT c CHECK x IN (SELECT 1 UNION ALL SELECT 2);
INSERT INTO t_union_all SETTINGS async_insert = 0 VALUES (1);
INSERT INTO t_union_all SETTINGS async_insert = 0 VALUES (3); -- { serverError VIOLATED_CONSTRAINT }

CREATE TABLE t_union_distinct (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05878_t_union_distinct', 'r1') ORDER BY x;
ALTER TABLE t_union_distinct ADD CONSTRAINT c CHECK x IN (SELECT 1 UNION DISTINCT SELECT 2);
INSERT INTO t_union_distinct SETTINGS async_insert = 0 VALUES (2);
INSERT INTO t_union_distinct SETTINGS async_insert = 0 VALUES (3); -- { serverError VIOLATED_CONSTRAINT }

CREATE TABLE t_intersect (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05878_t_intersect', 'r1') ORDER BY x;
ALTER TABLE t_intersect ADD CONSTRAINT c CHECK x IN (SELECT number FROM numbers(3) INTERSECT SELECT number FROM numbers(1, 5));
INSERT INTO t_intersect SETTINGS async_insert = 0 VALUES (1);
INSERT INTO t_intersect SETTINGS async_insert = 0 VALUES (0); -- { serverError VIOLATED_CONSTRAINT }

CREATE TABLE t_except (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05878_t_except', 'r1') ORDER BY x;
ALTER TABLE t_except ADD CONSTRAINT c CHECK x IN (SELECT number FROM numbers(4) EXCEPT SELECT 3);
INSERT INTO t_except SETTINGS async_insert = 0 VALUES (0);
INSERT INTO t_except SETTINGS async_insert = 0 VALUES (3); -- { serverError VIOLATED_CONSTRAINT }

SELECT 'alter', (SELECT count() FROM t_union_all), (SELECT count() FROM t_union_distinct), (SELECT count() FROM t_intersect), (SELECT count() FROM t_except);
