-- Tags: shard

-- An INTERPOLATE target whose alias is the name of another column of the query: https://github.com/ClickHouse/ClickHouse/issues/126941

-- The target is named like the aggregate `count()`.
SELECT n, count() AS c, n AS `count()` FROM (SELECT number AS n FROM numbers(3)) GROUP BY n ORDER BY n WITH FILL STEP 1 INTERPOLATE (`count()` AS `count()` + 1);
SELECT n, count() AS c, toUInt64(n * 2) AS `count()` FROM (SELECT number * 3 AS n FROM numbers(3)) GROUP BY n ORDER BY n WITH FILL STEP 1 INTERPOLATE (`count()` AS `count()` + 1);

-- The target is the aggregate itself.
SELECT n, count() AS `count()` FROM (SELECT number * 3 AS n FROM numbers(3)) GROUP BY n ORDER BY n WITH FILL STEP 1 INTERPOLATE (`count()` AS `count()` + 10);

-- The target is named like a constant.
SELECT n, 5 AS `5_UInt8` FROM (SELECT number * 3 AS n FROM numbers(3)) ORDER BY n WITH FILL STEP 1 INTERPOLATE (`5_UInt8` AS `5_UInt8` + 5);

-- Another target reads the column the first one is named like.
SELECT n, count() AS c, toString(n) AS `count()` FROM (SELECT number * 3 AS n FROM numbers(3)) GROUP BY n ORDER BY n WITH FILL STEP 1 INTERPOLATE (`count()`, c AS c + 10);

-- LIMIT AFTER reads the interpolated values.
SELECT n, count() AS `count()` FROM (SELECT number * 3 AS n FROM numbers(3)) GROUP BY n ORDER BY n WITH FILL STEP 1 INTERPOLATE (`count()` AS `count()` + 10) LIMIT 2 AFTER `count()` >= 11;

-- Distributed.
SELECT number * 3 AS n, count() AS `count()` FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY n ORDER BY n WITH FILL STEP 1 INTERPOLATE (`count()` AS `count()` + 10);

-- A target that is also an ORDER BY column is still rejected.
SELECT n, count() AS c, toUInt64(n * 2) AS `count()` FROM (SELECT number * 3 AS n FROM numbers(3)) GROUP BY n ORDER BY `count()`, n WITH FILL STEP 1 INTERPOLATE (`count()` AS `count()` + 1); -- { serverError INVALID_WITH_FILL_EXPRESSION }
SELECT n, count() AS `count()` FROM (SELECT number * 3 AS n FROM numbers(3)) GROUP BY n ORDER BY 2, n WITH FILL STEP 1 INTERPOLATE (`count()` AS `count()` + 10); -- { serverError INVALID_WITH_FILL_EXPRESSION }
