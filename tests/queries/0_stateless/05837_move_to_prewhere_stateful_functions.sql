-- Tags: no-random-settings, no-parallel-replicas
-- no-random-settings: the values of `rowNumberInAllBlocks` depend on the order the rows are read in.
-- no-parallel-replicas: `rowNumberInAllBlocks` counts only the rows of the local read.

-- A stateful function must see the same rows with and without `optimize_move_to_prewhere`.

SET max_threads = 1;

DROP TABLE IF EXISTS t_prewhere_stateful;
CREATE TABLE t_prewhere_stateful (a UInt64, b String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_prewhere_stateful SELECT number, toString(number) FROM numbers(10);

SELECT '-- stateful function in a subquery';
SELECT * FROM (SELECT rowNumberInAllBlocks() AS n, a, b FROM t_prewhere_stateful) WHERE a > 6 ORDER BY a;
SELECT * FROM (SELECT rowNumberInAllBlocks() AS n, a, b FROM t_prewhere_stateful) WHERE a > 6 ORDER BY a SETTINGS optimize_move_to_prewhere = 0;

SELECT '-- stateful function in the condition';
SELECT a, b FROM t_prewhere_stateful WHERE rowNumberInAllBlocks() >= 2 AND a % 2 = 0 ORDER BY a;
SELECT a, b FROM t_prewhere_stateful WHERE rowNumberInAllBlocks() >= 2 AND a % 2 = 0 ORDER BY a SETTINGS optimize_move_to_prewhere = 0;

SELECT '-- stateful filter over a filtered subquery';
SELECT a FROM (SELECT a FROM t_prewhere_stateful WHERE a % 2 = 0) WHERE rowNumberInAllBlocks() < 3 ORDER BY a;
SELECT a FROM (SELECT a FROM t_prewhere_stateful WHERE a % 2 = 0) WHERE rowNumberInAllBlocks() < 3 ORDER BY a SETTINGS optimize_move_to_prewhere = 0;

DROP TABLE t_prewhere_stateful;

DROP TABLE IF EXISTS t_prewhere_stateful_default;
CREATE TABLE t_prewhere_stateful_default (a UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_prewhere_stateful_default SELECT number FROM numbers(10);
ALTER TABLE t_prewhere_stateful_default ADD COLUMN n UInt64 DEFAULT rowNumberInAllBlocks();
ALTER TABLE t_prewhere_stateful_default ADD COLUMN m UInt64 DEFAULT n + 100;
ALTER TABLE t_prewhere_stateful_default ADD COLUMN tup Tuple(x UInt64, y String) DEFAULT tuple(rowNumberInAllBlocks(), 'z');

SELECT '-- stateful default of a missing column';
SELECT n FROM t_prewhere_stateful_default WHERE a >= 5 ORDER BY a;
SELECT n FROM t_prewhere_stateful_default WHERE a >= 5 ORDER BY a SETTINGS optimize_move_to_prewhere = 0;
SELECT m FROM t_prewhere_stateful_default WHERE a >= 5 ORDER BY a;
SELECT m FROM t_prewhere_stateful_default WHERE a >= 5 ORDER BY a SETTINGS optimize_move_to_prewhere = 0;
SELECT tup.x FROM t_prewhere_stateful_default WHERE a >= 5 ORDER BY a;
SELECT tup.x FROM t_prewhere_stateful_default WHERE a >= 5 ORDER BY a SETTINGS optimize_move_to_prewhere = 0;

DROP TABLE t_prewhere_stateful_default;

DROP TABLE IF EXISTS t_prewhere_stateful_dropped;
CREATE TABLE t_prewhere_stateful_dropped (a UInt64, n UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_prewhere_stateful_dropped SELECT number, 0 FROM numbers(10);
SYSTEM STOP MERGES t_prewhere_stateful_dropped;
SET mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_prewhere_stateful_dropped DROP COLUMN n;
ALTER TABLE t_prewhere_stateful_dropped ADD COLUMN n UInt64 DEFAULT rowNumberInAllBlocks();

SELECT '-- stateful default of a column dropped by a pending mutation';
SELECT n FROM t_prewhere_stateful_dropped WHERE a >= 5 ORDER BY a SETTINGS apply_mutations_on_fly = 1;
SELECT n FROM t_prewhere_stateful_dropped WHERE a >= 5 ORDER BY a SETTINGS apply_mutations_on_fly = 1, optimize_move_to_prewhere = 0;

DROP TABLE t_prewhere_stateful_dropped;
