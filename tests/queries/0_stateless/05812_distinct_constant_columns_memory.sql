-- DISTINCT must not copy a constant column for the rows it drops.

SELECT length(s) FROM (SELECT DISTINCT repeat('a', 1000000) AS s FROM numbers(100000)) SETTINGS max_memory_usage = '1Gi', max_block_size = 65409;
SELECT length(s) FROM (SELECT DISTINCT toFixedString('a', 1048577) AS s FROM numbers(100000)) SETTINGS max_memory_usage = '1Gi', max_block_size = 65409;

DROP TABLE IF EXISTS t_distinct_const;
CREATE TABLE t_distinct_const (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_distinct_const SELECT number % 3 FROM numbers(100000);
SELECT k, length(s) FROM (SELECT DISTINCT k, repeat('a', 1000000) AS s FROM t_distinct_const) ORDER BY k SETTINGS optimize_distinct_in_order = 1, max_memory_usage = '1Gi', max_block_size = 65409;
DROP TABLE t_distinct_const;
