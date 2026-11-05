-- Tags: zookeeper, no-shared-merge-tree
-- no-shared-merge-tree: the replicated arm checks that `IN` still requires `allow_nondeterministic_mutations`, which SharedMergeTree does not.

-- With `mutations_execute_subqueries_on_initiator`, the subqueries of `IN` and `exists` are executed when the mutation
-- is submitted, so the mutation keeps working after the tables they read are dropped.

SET mutations_execute_subqueries_on_initiator = 1;
SET mutations_max_literal_size_to_replace = 16384;
SET mutations_sync = 0;

DROP TABLE IF EXISTS t_in;
DROP TABLE IF EXISTS t_in_src;

CREATE TABLE t_in (k UInt64, v UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_in SELECT number, number, toString(number) FROM numbers(100);
CREATE TABLE t_in_src (k UInt64, s String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_in_src SELECT number, toString(number) FROM numbers(5);
INSERT INTO t_in_src VALUES (90, 'x');

SYSTEM STOP MERGES t_in;
SELECT '-- IN in the predicate, in an assignment, with several columns, the same subquery twice';
ALTER TABLE t_in DELETE WHERE k IN (SELECT k FROM t_in_src);
ALTER TABLE t_in UPDATE v = k IN (SELECT k + 10 FROM t_in_src) WHERE k < 20;
ALTER TABLE t_in DELETE WHERE (k, s) IN (SELECT k + 20, toString(k + 20) FROM t_in_src);
ALTER TABLE t_in UPDATE s = 'y' WHERE k IN (SELECT k + 30 FROM t_in_src) OR v IN (SELECT k + 30 FROM t_in_src);
DELETE FROM t_in WHERE k IN (SELECT k + 40 FROM t_in_src) SETTINGS lightweight_deletes_sync = 0;
DROP TABLE t_in_src;
SYSTEM START MERGES t_in;
ALTER TABLE t_in UPDATE v = 1000 WHERE k = 99 SETTINGS mutations_sync = 2;

SELECT count(), sum(v), countIf(s = 'y'), countIf(k = 99 AND v = 1000) FROM t_in;
SELECT k, v, s FROM t_in WHERE k < 50 AND (k < 15 OR v != k OR s != toString(k)) ORDER BY k;
SELECT replaceRegexpAll(replaceAll(command, currentDatabase(), 'default'), '\\[[^\\]]+\\]', '[...]'), is_done FROM system.mutations WHERE database = currentDatabase() AND table = 't_in' ORDER BY length(mutation_id), mutation_id;
-- Both occurrences of the subquery are replaced with the same result.
SELECT length(sets), arrayUniq(sets) FROM (SELECT extractAll(command, '\\[[^\\]]+\\]') AS sets FROM system.mutations WHERE database = currentDatabase() AND table = 't_in' AND command LIKE '%s = \'y\'%');

SELECT '-- exists';
DROP TABLE IF EXISTS t_exists;
DROP TABLE IF EXISTS t_exists_src;
CREATE TABLE t_exists (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_exists SELECT number, 0 FROM numbers(10);
CREATE TABLE t_exists_src (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_exists_src VALUES (3);

SYSTEM STOP MERGES t_exists;
ALTER TABLE t_exists UPDATE v = v + 1 WHERE exists(SELECT 1 FROM t_exists_src WHERE k = 3);
ALTER TABLE t_exists UPDATE v = v + 10 WHERE exists(SELECT 1 FROM t_exists_src WHERE k = 4);
-- Only whether the subquery returns rows is computed, not its columns.
ALTER TABLE t_exists UPDATE v = v + 100 WHERE k = 1 AND exists(SELECT throwIf(number = 0) FROM numbers(1));
DROP TABLE t_exists_src;
SYSTEM START MERGES t_exists;
ALTER TABLE t_exists UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2;

SELECT k, v FROM t_exists ORDER BY k;
SELECT replaceRegexpAll(replaceAll(command, currentDatabase(), 'default'), '\\[[^\\]]+\\]', '[...]'), is_done FROM system.mutations WHERE database = currentDatabase() AND table = 't_exists' ORDER BY length(mutation_id), mutation_id;

SELECT '-- The same result as the subquery executed by the mutation';
DROP TABLE IF EXISTS t_kept;
DROP TABLE IF EXISTS t_snapshot;
DROP TABLE IF EXISTS t_types_src;
CREATE TABLE t_kept (k UInt64, s String, n Nullable(UInt64), v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_kept VALUES (1, '01', 1, 0), (2, '1', 2, 0), (3, 'x', NULL, 0), (4, '4', 4, 0), (5, '5', 5, 0);
CREATE TABLE t_snapshot AS t_kept;
INSERT INTO t_snapshot SELECT * FROM t_kept;
CREATE TABLE t_types_src (x UInt8, n Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_types_src VALUES (1, 2), (7, NULL);

ALTER TABLE t_kept DELETE WHERE s IN (SELECT x FROM t_types_src) SETTINGS mutations_execute_subqueries_on_initiator = 0, mutations_sync = 2;
ALTER TABLE t_kept UPDATE v = v + 1 WHERE n NOT IN (SELECT n FROM t_types_src) SETTINGS mutations_execute_subqueries_on_initiator = 0, mutations_sync = 2;
ALTER TABLE t_kept UPDATE v = v + 10 WHERE k IN (SELECT x FROM t_types_src WHERE x > 100) SETTINGS mutations_execute_subqueries_on_initiator = 0, mutations_sync = 2;

SYSTEM STOP MERGES t_snapshot;
ALTER TABLE t_snapshot DELETE WHERE s IN (SELECT x FROM t_types_src);
ALTER TABLE t_snapshot UPDATE v = v + 1 WHERE n NOT IN (SELECT n FROM t_types_src);
ALTER TABLE t_snapshot UPDATE v = v + 10 WHERE k IN (SELECT x FROM t_types_src WHERE x > 100);
DROP TABLE t_types_src;
SYSTEM START MERGES t_snapshot;
ALTER TABLE t_snapshot UPDATE v = v WHERE 1 SETTINGS mutations_sync = 2;

SELECT * FROM t_kept ORDER BY k;
SELECT (SELECT groupArray((k, s, n, v)) FROM (SELECT * FROM t_kept ORDER BY k)) = (SELECT groupArray((k, s, n, v)) FROM (SELECT * FROM t_snapshot ORDER BY k));
SELECT replaceRegexpAll(replaceAll(command, currentDatabase(), 'default'), '\\[[^\\]]+\\]', '[...]') FROM system.mutations WHERE database = currentDatabase() AND table = 't_snapshot' ORDER BY length(mutation_id), mutation_id;

SELECT '-- Kept as a subquery';
DROP TABLE IF EXISTS t_large;
DROP TABLE IF EXISTS t_large_src;
DROP TABLE IF EXISTS t_variant_src;
CREATE TABLE t_large (k UInt64, e Tuple(), v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_large SELECT number, tuple(), 0 FROM numbers(200);
CREATE TABLE t_large_src (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_large_src SELECT number FROM numbers(30);
CREATE TABLE t_variant_src (x Variant(UInt64, String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_variant_src VALUES (150), ('a');

-- 30 elements take 30 bytes, but more than 64 characters of the literal.
ALTER TABLE t_large UPDATE v = v + 1 WHERE k IN (SELECT toUInt8(k + 100) FROM t_large_src) SETTINGS mutations_max_literal_size_to_replace = 64, mutations_sync = 2;
-- The empty tuples take no bytes, so the number of rows bounds the result.
ALTER TABLE t_large UPDATE v = v + 10 WHERE e IN (SELECT tuple() FROM numbers(2000000)) AND k = 199 SETTINGS mutations_sync = 2;
ALTER TABLE t_large UPDATE v = v + 100 WHERE k IN (SELECT number FROM numbers(2000000)) AND k = 199 SETTINGS mutations_max_literal_size_to_replace = 100000000, mutations_sync = 2;
ALTER TABLE t_large UPDATE v = v + 1000 WHERE k IN (SELECT x FROM t_variant_src) SETTINGS mutations_sync = 2;

SELECT sum(v), countIf(v = 1), countIf(v = 1000), anyIf(v, k = 199) FROM t_large;
SELECT replaceRegexpAll(replaceAll(command, currentDatabase(), 'default'), '\\[[^\\]]+\\]', '[...]'), is_done FROM system.mutations WHERE database = currentDatabase() AND table = 't_large' ORDER BY length(mutation_id), mutation_id;

SELECT '-- The same subquery is executed once, and pulling stops at the limit';
DROP TABLE IF EXISTS t_once;
-- Empty on purpose: the mutations below never execute a kept subquery.
CREATE TABLE t_once (k UInt64, e Tuple(), v UInt64) ENGINE = MergeTree ORDER BY k;

ALTER TABLE t_once UPDATE v = 1 WHERE k IN (SELECT rand64()) OR v IN (SELECT rand64()) SETTINGS mutations_sync = 2;
SELECT length(sets), arrayUniq(sets) FROM (SELECT extractAll(command, '\\[[^\\]]+\\]') AS sets FROM system.mutations WHERE database = currentDatabase() AND table = 't_once' AND command LIKE '%v = 1%');

-- A subquery that is read past one million rows throws, so these are accepted only if pulling stops at the limit.
ALTER TABLE t_once DELETE WHERE k IN (SELECT number + throwIf(number >= 1000000) FROM numbers(2000000) SETTINGS max_block_size = 8192, max_threads = 1) SETTINGS mutations_sync = 2;
-- Empty tuples take no bytes, so only the number of rows stops it.
ALTER TABLE t_once DELETE WHERE e IN (SELECT tuple() FROM numbers(2000000) WHERE throwIf(number >= 1000000) = 0 SETTINGS max_block_size = 8192, max_threads = 1) SETTINGS mutations_sync = 2;
SELECT replaceRegexpAll(replaceAll(command, currentDatabase(), 'default'), '\\[[^\\]]+\\]', '[...]'), is_done FROM system.mutations WHERE database = currentDatabase() AND table = 't_once' ORDER BY length(mutation_id), mutation_id;

SELECT '-- ReplicatedMergeTree';
DROP TABLE IF EXISTS t_replicated SYNC;
DROP TABLE IF EXISTS t_replicated_src;
CREATE TABLE t_replicated (k UInt64, v UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/05508_mutations_execute_set_subqueries_on_initiator', '1') ORDER BY k;
INSERT INTO t_replicated SELECT number, 0 FROM numbers(10);
CREATE TABLE t_replicated_src (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_replicated_src VALUES (3);

-- A result of `exists` is a constant, so the mutation is deterministic.
ALTER TABLE t_replicated UPDATE v = 1 WHERE exists(SELECT 1 FROM t_replicated_src WHERE k = 3) SETTINGS mutations_sync = 2;
ALTER TABLE t_replicated UPDATE v = 2 WHERE k IN (SELECT k FROM t_replicated_src) SETTINGS mutations_sync = 2; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_replicated UPDATE v = 2 WHERE k IN (SELECT k FROM t_replicated_src) SETTINGS mutations_sync = 2, allow_nondeterministic_mutations = 1;

SELECT k, v FROM t_replicated ORDER BY k;
SELECT replaceRegexpAll(replaceAll(command, currentDatabase(), 'default'), '\\[[^\\]]+\\]', '[...]'), is_done FROM system.mutations WHERE database = currentDatabase() AND table = 't_replicated' ORDER BY mutation_id;

DROP TABLE t_in;
DROP TABLE t_exists;
DROP TABLE t_kept;
DROP TABLE t_snapshot;
DROP TABLE t_large;
DROP TABLE t_large_src;
DROP TABLE t_variant_src;
DROP TABLE t_once;
DROP TABLE t_replicated SYNC;
DROP TABLE t_replicated_src;
