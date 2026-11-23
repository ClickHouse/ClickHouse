-- https://github.com/ClickHouse/ClickHouse/issues/133337
SELECT toTypeName(x), x, x = true FROM (SELECT true AS x UNION ALL SELECT toUInt8(2) AS x) ORDER BY x;
SELECT toTypeName(x), x, x = true FROM (SELECT toUInt8(2) AS x UNION ALL SELECT true AS x) ORDER BY x;
SELECT toTypeName(x), x FROM (SELECT true AS x UNION ALL SELECT false AS x) ORDER BY x;
SELECT toTypeName(x), x FROM (SELECT [true] AS x UNION ALL SELECT [toUInt8(2)]) ORDER BY x;
SELECT toTypeName(x), x FROM (SELECT CAST(true, 'Nullable(Bool)') AS x UNION ALL SELECT toUInt8(2)) ORDER BY x;
SELECT toTypeName(x), x FROM (SELECT tuple(true) AS x UNION ALL SELECT tuple(toUInt8(2))) ORDER BY x;
SELECT toTypeName(x), x FROM (SELECT map('a', true) AS x UNION ALL SELECT map('a', toUInt8(2))) ORDER BY toString(x);
SELECT toTypeName(x), x FROM (SELECT toLowCardinality(true) AS x UNION ALL SELECT toLowCardinality(toUInt8(2))) ORDER BY x;
SELECT toTypeName(x), x FROM (SELECT true AS x INTERSECT SELECT toUInt8(1));
SELECT toTypeName(if(number = 0, true, toUInt8(2))), if(number = 0, true, toUInt8(2)) FROM numbers(2);
SELECT toTypeName(if(number = 1, toUInt8(2), true)), if(number = 1, toUInt8(2), true) FROM numbers(2);
SELECT toTypeName(if(number = 0, true, false)), toTypeName(multiIf(number = 0, true, toUInt8(2))), toTypeName(coalesce(CAST(number, 'Nullable(Bool)'), toUInt8(2))) FROM numbers(1);
SELECT [true, 2], toTypeName([true, 2]), [2, true], toTypeName([2, true]), [true, false], toTypeName([true, false]);
SELECT toTypeName(v), v FROM (SELECT true AS v) a FULL JOIN (SELECT toUInt8(2) AS v) b USING v ORDER BY v;
SELECT toTypeName(v), v FROM (SELECT toUInt8(2) AS v) a FULL JOIN (SELECT true AS v) b USING v ORDER BY v;
SELECT toTypeName(a.v), toTypeName(b.v), toTypeName(v) FROM (SELECT true AS v) a INNER JOIN (SELECT toUInt8(1) AS v) b USING v;
WITH RECURSIVE r AS (SELECT true AS x UNION ALL SELECT toUInt8(x + 1) FROM r WHERE x < 2) SELECT toTypeName(x), x FROM r ORDER BY x;
