-- Tags: shard
-- https://github.com/ClickHouse/ClickHouse/issues/129888

DROP TABLE IF EXISTS t_gs_empty_set_filter;
CREATE TABLE t_gs_empty_set_filter (a Int32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_gs_empty_set_filter SELECT number FROM numbers(10);

-- A filter that reads no column stays above a GROUPING SETS aggregation with a key-less set.
SELECT count() FROM (SELECT toNullable(1) FROM t_gs_empty_set_filter GROUP BY GROUPING SETS ((), (identity(toInt256(1023))))) WHERE identity(NULL);
SELECT count() FROM (SELECT a FROM t_gs_empty_set_filter GROUP BY GROUPING SETS ((), (a))) WHERE identity(0);
SELECT count() FROM (SELECT a, count() FROM t_gs_empty_set_filter GROUP BY GROUPING SETS ((), (a)) HAVING identity(0));
SELECT count() FROM (SELECT count() FROM t_gs_empty_set_filter GROUP BY GROUPING SETS (('k'), (a))) WHERE identity(0);
SELECT count() FROM (SELECT a FROM remote('127.0.0.{1,2}', currentDatabase(), t_gs_empty_set_filter) GROUP BY GROUPING SETS ((), (a))) WHERE identity(0);

-- It is still pushed below the aggregation when every set has a key.
SELECT arrayFirstIndex(x -> x ILIKE '%Filter%', lines) > arrayFirstIndex(x -> x ILIKE '%Aggregating%', lines)
FROM (SELECT groupArray(explain) AS lines FROM (EXPLAIN SELECT a FROM (SELECT a FROM t_gs_empty_set_filter GROUP BY GROUPING SETS ((a), (a, a + 1))) WHERE identity(0)));

DROP TABLE t_gs_empty_set_filter;
