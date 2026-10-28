-- Tags: no-fasttest, log-engine
-- An INSERT that writes no rows into a table on object storage must not leave a reference to a blob it never uploaded.

DROP TABLE IF EXISTS t_empty_insert_middle;
CREATE TABLE t_empty_insert_middle (n UInt32) ENGINE = StripeLog SETTINGS storage_policy = 's3_cache';
INSERT INTO t_empty_insert_middle SELECT 1;
INSERT INTO t_empty_insert_middle SELECT 1 LIMIT 0;
INSERT INTO t_empty_insert_middle SELECT toUInt32(number) FROM numbers(10) WHERE number > 100;
INSERT INTO t_empty_insert_middle SELECT 2;
SELECT n FROM t_empty_insert_middle ORDER BY n;
DROP TABLE t_empty_insert_middle;

-- An empty first INSERT still creates the data file.
DROP TABLE IF EXISTS t_empty_insert_first;
CREATE TABLE t_empty_insert_first (n UInt32) ENGINE = StripeLog SETTINGS storage_policy = 's3_no_cache';
INSERT INTO t_empty_insert_first SELECT 1 LIMIT 0;
INSERT INTO t_empty_insert_first SELECT 3;
SELECT n FROM t_empty_insert_first;
DROP TABLE t_empty_insert_first;
