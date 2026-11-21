-- Tags: shard

-- Result limits (`max_result_rows`, `max_result_bytes`) apply only to the final result of the query.
-- A shard must not apply them to its partial result: `INSERT SELECT` from a distributed source used to insert
-- only the first block of every shard with `result_overflow_mode = 'break'` and to fail with 'throw'.

DROP TABLE IF EXISTS src_05523;
DROP TABLE IF EXISTS dst_05523;
DROP TABLE IF EXISTS dist_05523;

CREATE TABLE src_05523 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dst_05523 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dist_05523 AS src_05523 ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), src_05523);

-- More than one block, so that a shard applying the limit stops after its first block.
INSERT INTO src_05523 SELECT number FROM numbers(200000);

SET prefer_localhost_replica = 0;

SELECT 'local, break';
INSERT INTO dst_05523 SELECT * FROM src_05523 SETTINGS max_result_rows = 10, result_overflow_mode = 'break';
SELECT count() FROM dst_05523;
TRUNCATE TABLE dst_05523;

SELECT 'remote, rows, break';
INSERT INTO dst_05523 SELECT * FROM remote('127.0.0.{1,2}', currentDatabase(), src_05523) SETTINGS max_result_rows = 10, result_overflow_mode = 'break';
SELECT count() FROM dst_05523;
TRUNCATE TABLE dst_05523;

SELECT 'remote, rows, throw';
INSERT INTO dst_05523 SELECT * FROM remote('127.0.0.{1,2}', currentDatabase(), src_05523) SETTINGS max_result_rows = 10, result_overflow_mode = 'throw';
SELECT count() FROM dst_05523;
TRUNCATE TABLE dst_05523;

SELECT 'remote, bytes, break';
INSERT INTO dst_05523 SELECT * FROM remote('127.0.0.{1,2}', currentDatabase(), src_05523) SETTINGS max_result_bytes = 100, result_overflow_mode = 'break';
SELECT count() FROM dst_05523;
TRUNCATE TABLE dst_05523;

SELECT 'Distributed, rows, break';
INSERT INTO dst_05523 SELECT * FROM dist_05523 SETTINGS max_result_rows = 10, result_overflow_mode = 'break';
SELECT count() FROM dst_05523;
TRUNCATE TABLE dst_05523;

-- The initiator still applies the limits to the final result.
SELECT 'final result, break';
SELECT x FROM remote('127.0.0.{1,2}', currentDatabase(), src_05523) ORDER BY x LIMIT 100 SETTINGS max_result_rows = 10, result_overflow_mode = 'break', max_block_size = 5;
SELECT 'final result, throw';
SELECT x FROM remote('127.0.0.{1,2}', currentDatabase(), src_05523) GROUP BY x SETTINGS max_result_rows = 10, result_overflow_mode = 'throw' FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT x FROM remote('127.0.0.{1,2}', currentDatabase(), src_05523) SETTINGS max_result_rows = 10, result_overflow_mode = 'throw' FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }
SELECT x FROM remote('127.0.0.{1,2}', currentDatabase(), src_05523) SETTINGS max_result_bytes = 100, result_overflow_mode = 'throw' FORMAT Null; -- { serverError TOO_MANY_ROWS_OR_BYTES }

DROP TABLE dist_05523;
DROP TABLE dst_05523;
DROP TABLE src_05523;
