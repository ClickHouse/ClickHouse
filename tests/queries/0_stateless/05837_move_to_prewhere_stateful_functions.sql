-- Tags: no-random-settings, no-parallel-replicas

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
CREATE TABLE t_prewhere_stateful_default (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_prewhere_stateful_default SELECT number FROM numbers(10);
ALTER TABLE t_prewhere_stateful_default ADD COLUMN n UInt64 DEFAULT rowNumberInAllBlocks();
ALTER TABLE t_prewhere_stateful_default ADD COLUMN m UInt64 DEFAULT n + 100;

SELECT '-- stateful default of a missing column';
SELECT n FROM t_prewhere_stateful_default WHERE a >= 5 ORDER BY a;
SELECT n FROM t_prewhere_stateful_default WHERE a >= 5 ORDER BY a SETTINGS optimize_move_to_prewhere = 0;
SELECT m FROM t_prewhere_stateful_default WHERE a >= 5 ORDER BY a;
SELECT m FROM t_prewhere_stateful_default WHERE a >= 5 ORDER BY a SETTINGS optimize_move_to_prewhere = 0;

DROP TABLE t_prewhere_stateful_default;
