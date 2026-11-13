-- `count(*)` reads no column, so adding a column keeps a hypothetical aggregate projection applicable, as for `count()`

SET enable_parallel_replicas = 0, optimize_use_projections = 1;

DROP TABLE IF EXISTS t_count_star;
CREATE TABLE t_count_star (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 100;
INSERT INTO t_count_star SELECT number % 10, number FROM numbers(1000);

CREATE HYPOTHETICAL PROJECTION p_count ON t_count_star (SELECT a, count() GROUP BY a);
CREATE HYPOTHETICAL PROJECTION p_count_star ON t_count_star (SELECT a, count(*) GROUP BY a);
ALTER TABLE t_count_star ADD COLUMN c UInt64;
SELECT trim(explain) FROM (EXPLAIN WHATIF SELECT a, count() FROM t_count_star GROUP BY a) WHERE explain LIKE 'With %' OR explain LIKE '  status:%';

DROP TABLE t_count_star;
