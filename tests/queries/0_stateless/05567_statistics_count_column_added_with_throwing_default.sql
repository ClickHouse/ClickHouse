-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks EXPLAIN of plans with reading steps replaced by prepared sources.

-- `count(column)` answered from statistics must still read a part that does not store the column: a read evaluates
-- the `DEFAULT` of a column added after the part was written, and that can throw.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET optimize_trivial_count_query = 0, optimize_syntax_fuse_functions = 0;
SET materialize_statistics_on_insert = 1;
SET use_statistics_for_sum_avg_aggregation = 1;

DROP TABLE IF EXISTS t_count_late_default;

CREATE TABLE t_count_late_default (x Int32) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic';

SYSTEM STOP MERGES t_count_late_default;

INSERT INTO t_count_late_default VALUES (1);
ALTER TABLE t_count_late_default ADD COLUMN y Int32 DEFAULT intDiv(10, x - 1);

SELECT sum(x), count(y) FROM t_count_late_default SETTINGS use_statistics_for_sum_avg_aggregation = 0; -- { serverError ILLEGAL_DIVISION }
SELECT sum(x), count(y) FROM t_count_late_default; -- { serverError ILLEGAL_DIVISION }

-- a part that stores the column is answered from statistics
INSERT INTO t_count_late_default VALUES (2, 3);
SELECT sum(x), count(x) FROM t_count_late_default;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT sum(x), count(y) FROM t_count_late_default);

DROP TABLE t_count_late_default;
