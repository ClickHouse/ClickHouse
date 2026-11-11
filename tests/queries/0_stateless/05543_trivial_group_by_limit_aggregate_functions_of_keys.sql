-- Tags: shard
-- The limit on groups for `GROUP BY ... LIMIT` without aggregate functions also applies to aggregations in views. So an aggregate
-- function of a key that is a function is not replaced with the key if the limit would be used then.

SET enable_positional_arguments = 1;
SET optimize_trivial_group_by_limit_query = 1, optimize_aggregators_of_group_by_keys = 1;
SET max_block_size = 10, max_threads = 1, max_rows_to_group_by = 0, group_by_two_level_threshold = 0, group_by_two_level_threshold_bytes = 0;

DROP VIEW IF EXISTS v;
-- Every key has 10 rows or 11, and the first 50 keys have 11.
CREATE VIEW v AS SELECT number % 100 AS k, count() AS c FROM numbers(1050) GROUP BY k HAVING c = 10;

DROP TABLE IF EXISTS r;
CREATE TABLE r (x UInt64, m UInt64) ENGINE = Memory;
INSERT INTO r SELECT k + 1 AS x, max(x) AS m FROM v GROUP BY 1 LIMIT 60;
INSERT INTO r SELECT k + 1 AS x, max(x) AS m FROM v GROUP BY x LIMIT 60;
INSERT INTO r SELECT k + 1 AS x, max(x) AS m FROM remote('127.0.0.{2,3}', currentDatabase(), v) GROUP BY 1 LIMIT 60;
INSERT INTO r SELECT k + 1 AS x, max(x) AS m FROM merge(currentDatabase(), '^v$') GROUP BY 1 LIMIT 60;
SELECT count(), uniqExact(x), min(x), max(x), countIf(x != m) FROM r;

DROP TABLE r;
DROP VIEW v;
