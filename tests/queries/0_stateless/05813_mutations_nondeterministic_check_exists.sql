-- Tags: zookeeper, no-shared-catalog, no-shared-merge-tree
-- no-shared-catalog: STOP MERGES will only stop them on the current replica, the second one will continue to merge
-- no-shared-merge-tree: only `ReplicatedMergeTree` rejects nondeterministic mutations without `allow_nondeterministic_mutations`

-- A mutation whose condition has `exists((subquery))` is a mutation with a subquery, the same as one with `IN (subquery)`.
-- Where mutations with a subquery are rejected, i.e. on replicated tables without `allow_nondeterministic_mutations`
-- and when mutations are applied on the fly, `exists` must be rejected with the same error, and not with an unknown function.

SELECT '-- replicated in';
DROP TABLE IF EXISTS t_replicated;
CREATE TABLE t_replicated (id UInt64, v UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated', 'r1') ORDER BY id;
INSERT INTO t_replicated VALUES (1, 10), (2, 20);
ALTER TABLE t_replicated UPDATE v = 0 WHERE id IN (SELECT 1) SETTINGS allow_nondeterministic_mutations = 0; -- { serverError BAD_ARGUMENTS }

SELECT '-- replicated exists';
ALTER TABLE t_replicated UPDATE v = 0 WHERE exists((SELECT * FROM system.one)) SETTINGS allow_nondeterministic_mutations = 0; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_replicated DELETE WHERE exists((SELECT * FROM system.one)) SETTINGS allow_nondeterministic_mutations = 0; -- { serverError BAD_ARGUMENTS }

SELECT '-- replicated allowed';
ALTER TABLE t_replicated UPDATE v = 0 WHERE exists((SELECT * FROM system.one)) SETTINGS allow_nondeterministic_mutations = 1, mutations_sync = 2;
SELECT id, v FROM t_replicated ORDER BY id;
DROP TABLE t_replicated;

SELECT '-- on fly in';
DROP TABLE IF EXISTS t_on_fly;
CREATE TABLE t_on_fly (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_on_fly VALUES (1, 10), (2, 20);
SYSTEM STOP MERGES t_on_fly;
ALTER TABLE t_on_fly UPDATE v = 0 WHERE id IN (SELECT 1) SETTINGS mutations_sync = 0;
SELECT id, v FROM t_on_fly ORDER BY id SETTINGS apply_mutations_on_fly = 1; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_on_fly;

SELECT '-- on fly exists';
CREATE TABLE t_on_fly (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_on_fly VALUES (1, 10), (2, 20);
SYSTEM STOP MERGES t_on_fly;
ALTER TABLE t_on_fly UPDATE v = 0 WHERE exists((SELECT * FROM system.one)) SETTINGS mutations_sync = 0;
SELECT id, v FROM t_on_fly ORDER BY id SETTINGS apply_mutations_on_fly = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- applied';
SYSTEM START MERGES t_on_fly;
ALTER TABLE t_on_fly UPDATE v = v WHERE 0 SETTINGS mutations_sync = 2;
SELECT id, v FROM t_on_fly ORDER BY id;
DROP TABLE t_on_fly;
