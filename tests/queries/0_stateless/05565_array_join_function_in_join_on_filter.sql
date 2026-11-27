-- rowNumberInBlock in a one-sided JOIN ON condition sees the rows that arrayJoin makes, as in WHERE, with and without plan optimizations

SET query_plan_lower_array_join_function = 1;
SET legacy_array_join_function_nondeterministic_evaluation = 0;
SET max_threads = 1;
-- rowNumberInBlock() = 1 has no columns, a swap of the tables moves it to the other side
SET query_plan_join_swap_table = 0;

SET query_plan_enable_optimizations = 1;
SELECT l.k, r.k FROM values('k UInt8', 1, 2) AS l INNER JOIN values('k UInt8', 1, 2) AS r ON l.k = r.k AND arrayJoin([l.k, l.k]) = rowNumberInBlock() ORDER BY ALL;
SELECT '--';
SELECT l.k, r.k FROM values('k UInt8', 1, 2) AS l INNER JOIN values('k UInt8', 1, 2) AS r ON l.k = r.k AND arrayJoin([l.k, l.k]) > 0 AND rowNumberInBlock() = 1 ORDER BY ALL;
SELECT '--';
SELECT l.k, r.k FROM values('k UInt8', 1, 2) AS l INNER JOIN values('k UInt8', 1, 2) AS r ON l.k = r.k WHERE arrayJoin([l.k, l.k]) > 0 AND rowNumberInBlock() = 1 ORDER BY ALL;
SELECT '-- no plan optimizations';
SET query_plan_enable_optimizations = 0;
SELECT l.k, r.k FROM values('k UInt8', 1, 2) AS l INNER JOIN values('k UInt8', 1, 2) AS r ON l.k = r.k AND arrayJoin([l.k, l.k]) = rowNumberInBlock() ORDER BY ALL;
SELECT '--';
SELECT l.k, r.k FROM values('k UInt8', 1, 2) AS l INNER JOIN values('k UInt8', 1, 2) AS r ON l.k = r.k AND arrayJoin([l.k, l.k]) > 0 AND rowNumberInBlock() = 1 ORDER BY ALL;
SELECT '--';
SELECT l.k, r.k FROM values('k UInt8', 1, 2) AS l INNER JOIN values('k UInt8', 1, 2) AS r ON l.k = r.k WHERE arrayJoin([l.k, l.k]) > 0 AND rowNumberInBlock() = 1 ORDER BY ALL;
