-- The branch tagged 'x' is filtered out by the outer WHERE: its IN set must not be built.
SELECT count() FROM (SELECT 'x' AS k, number AS n FROM numbers(10) WHERE n IN (SELECT throwIf(number = 0, 'dead set built') FROM numbers(1)) UNION ALL SELECT 'y' AS k, number AS n FROM numbers(10) WHERE n IN (SELECT number FROM numbers(5))) WHERE k = 'y';
SELECT count() FROM (SELECT 'x' AS k, number AS n FROM numbers(10) WHERE n IN (SELECT throwIf(number = 0, 'dead set built') FROM numbers(1)) UNION ALL SELECT 'y' AS k, number AS n FROM numbers(10) WHERE n IN (SELECT number FROM numbers(5))) WHERE k = 'y' SETTINGS query_plan_merge_filters = 0;
-- The 'x' branch is selected: its set is built.
SELECT count() FROM (SELECT 'x' AS k, number AS n FROM numbers(10) WHERE n IN (SELECT throwIf(number = 0, 'live set built') FROM numbers(1)) UNION ALL SELECT 'y' AS k, number AS n FROM numbers(10) WHERE n IN (SELECT number FROM numbers(5))) WHERE k = 'x'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
-- The set is used in the projection of the filtered-out branch.
SELECT k, f FROM (SELECT 'x' AS k, number IN (SELECT throwIf(number = 0, 'dead set built') FROM numbers(1)) AS f FROM numbers(10) UNION ALL SELECT 'y' AS k, 1 AS f FROM numbers(2)) WHERE k = 'y';
-- The totals row of the filtered-out branch passes the filter, so its input is still read.
SELECT k, n FROM (SELECT 'x' AS k, n FROM (SELECT number AS n FROM numbers(4) GROUP BY n WITH TOTALS) WHERE n IN (SELECT number FROM numbers(3)) UNION ALL SELECT 'y' AS k, number AS n FROM numbers(2)) WHERE k = 'y' ORDER BY n;
-- The filtered-out branch reads remote().
SELECT count() FROM (SELECT 'x' AS k, n FROM (SELECT number AS n FROM remote('127.0.0.{1,2}', numbers(10))) WHERE n IN (SELECT throwIf(number = 0, 'dead set built') FROM numbers(1)) UNION ALL SELECT 'y' AS k, number AS n FROM numbers(10) WHERE n IN (SELECT number FROM numbers(5))) WHERE k = 'y' SETTINGS prefer_localhost_replica = 0;
-- The filtered-out branch reads a subquery with its own IN set.
SELECT count() FROM (SELECT 'x' AS k, n FROM (SELECT number AS n FROM numbers(10) WHERE number IN (SELECT throwIf(number = 0, 'dead inner set built') FROM numbers(1))) WHERE n IN (SELECT throwIf(number = 0, 'dead outer set built') FROM numbers(1)) UNION ALL SELECT 'y' AS k, number AS n FROM numbers(10) WHERE n IN (SELECT number FROM numbers(5))) WHERE k = 'y';
