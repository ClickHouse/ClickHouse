-- rowNumberInBlock in a one-sided JOIN ON condition sees the rows that arrayJoin makes, as in WHERE

SET query_plan_lower_array_join_function = 1;
SET legacy_array_join_function_nondeterministic_evaluation = 0;
SET max_threads = 1;

SELECT l.k, r.k FROM values('k UInt8', 1, 2) AS l INNER JOIN values('k UInt8', 1, 2) AS r ON l.k = r.k AND arrayJoin([l.k, l.k]) = rowNumberInBlock() ORDER BY ALL;
SELECT '--';
SELECT l.k, r.k FROM values('k UInt8', 1, 2) AS l INNER JOIN values('k UInt8', 1, 2) AS r ON l.k = r.k AND arrayJoin([l.k, l.k]) > 0 AND rowNumberInBlock() = 1 ORDER BY ALL;
SELECT '--';
SELECT l.k, r.k FROM values('k UInt8', 1, 2) AS l INNER JOIN values('k UInt8', 1, 2) AS r ON l.k = r.k WHERE arrayJoin([l.k, l.k]) > 0 AND rowNumberInBlock() = 1 ORDER BY ALL;
