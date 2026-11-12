-- arrayJoin in additional_table_filters is planned as an ARRAY JOIN step

SET query_plan_lower_array_join_function = 1;
SET legacy_array_join_function_nondeterministic_evaluation = 0;
SET max_threads = 1;

DROP TABLE IF EXISTS t_aj_filter;
CREATE TABLE t_aj_filter (k UInt8) ENGINE = Memory;
INSERT INTO t_aj_filter VALUES (1), (2);

SELECT count() FROM (EXPLAIN SELECT k FROM t_aj_filter SETTINGS additional_table_filters = {'t_aj_filter': 'arrayJoin([1, 2]) > 0 AND rowNumberInBlock() = 1'})
WHERE explain LIKE '%ArrayJoin (ARRAY JOIN)%';

-- rowNumberInBlock sees the joined rows, as for WHERE
SELECT k FROM t_aj_filter ORDER BY k SETTINGS additional_table_filters = {'t_aj_filter': 'arrayJoin([1, 2]) > 0 AND rowNumberInBlock() = 1'};
SELECT k FROM t_aj_filter WHERE arrayJoin([1, 2]) > 0 AND rowNumberInBlock() = 1 ORDER BY k;

DROP TABLE t_aj_filter;
