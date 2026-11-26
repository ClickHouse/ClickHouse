-- Ordinary `GROUP BY` keys partition the clusters by their exact values, as in ordinary aggregation.
-- `IColumn::compareAt` treats some distinct values as equal: aggregate function states (also inside
-- `Variant` and `Dynamic`) and distinct NaN payloads. Rows that differ in such keys must not be merged
-- into one cluster. Every query below has two exact partitions of one row each, except for the last section.

SET enable_analyzer = 1;
SET allow_experimental_group_by_with_cluster = 1;
SET allow_suspicious_types_in_group_by = 1;

SELECT 'numeric cluster key';

SELECT min(number), count()
FROM (SELECT number, CAST(initializeAggregation('sumState', number), 'Variant(AggregateFunction(sum, UInt64), String)') AS g FROM numbers(2))
GROUP BY g, number WITH CLUSTER 1 ORDER BY 1;

SELECT min(number), count()
FROM (SELECT number, CAST(initializeAggregation('sumState', number), 'Dynamic') AS g FROM numbers(2))
GROUP BY g, number WITH CLUSTER 1 ORDER BY 1;

SELECT min(x), count()
FROM (SELECT reinterpretAsFloat64(toUInt64(9221120237041090561 + number)) AS g, number AS x FROM numbers(2))
GROUP BY g, x WITH CLUSTER 1 ORDER BY 1;

-- The same with `distance = 0` and with many rows per partition, which exercises bucket merging.
SELECT min(x), max(x), count()
FROM (SELECT reinterpretAsFloat64(toUInt64(9221120237041090561 + number % 2)) AS g, intDiv(number, 2) AS x FROM numbers(1000))
GROUP BY g, x WITH CLUSTER 1;

SELECT count()
FROM
(
    SELECT x, count()
    FROM (SELECT reinterpretAsFloat64(toUInt64(9221120237041090561 + number)) AS g, 1 AS x FROM numbers(2))
    GROUP BY g, x WITH CLUSTER 0
);

SELECT 'string cluster key';

SELECT min(s), count()
FROM (SELECT ['abc', 'abd'][number + 1] AS s, CAST(initializeAggregation('sumState', number), 'Variant(AggregateFunction(sum, UInt64), String)') AS g FROM numbers(2))
GROUP BY g, s WITH CLUSTER 1 ORDER BY 1;

SELECT min(s), count()
FROM (SELECT ['abc', 'abd'][number + 1] AS s, CAST(initializeAggregation('sumState', number), 'Dynamic') AS g FROM numbers(2))
GROUP BY g, s WITH CLUSTER 1 ORDER BY 1;

SELECT min(s), count()
FROM (SELECT ['abc', 'abd'][number + 1] AS s, reinterpretAsFloat64(toUInt64(9221120237041090561 + number)) AS g FROM numbers(2))
GROUP BY g, s WITH CLUSTER 1 ORDER BY 1;

SELECT '2D cluster key';

SELECT min(x), count()
FROM (SELECT number AS x, 0 AS y, CAST(initializeAggregation('sumState', number), 'Variant(AggregateFunction(sum, UInt64), String)') AS g FROM numbers(2))
GROUP BY g, (x, y) WITH CLUSTER 1 ORDER BY 1;

SELECT min(x), count()
FROM (SELECT number AS x, 0 AS y, reinterpretAsFloat64(toUInt64(9221120237041090561 + number)) AS g FROM numbers(2))
GROUP BY g, (x, y) WITH CLUSTER 1 ORDER BY 1;

SELECT 'equal ordinary keys still merge';

SELECT min(x), count()
FROM (SELECT reinterpretAsFloat64(toUInt64(9221120237041090561)) AS g, number AS x FROM numbers(2))
GROUP BY g, x WITH CLUSTER 1;

SELECT min(s), count()
FROM (SELECT ['abc', 'abd'][number + 1] AS s, CAST(initializeAggregation('sumState', 1::UInt64), 'Dynamic') AS g FROM numbers(2))
GROUP BY g, s WITH CLUSTER 1;

-- `-0.0` and `+0.0` are the same key in ordinary aggregation, so they are one partition here as well.
SELECT min(x), count()
FROM (SELECT if(number = 0, -0.0, 0.0) AS g, number AS x FROM numbers(2))
GROUP BY g, x WITH CLUSTER 1;

SELECT min(s), count()
FROM (SELECT ['abc', 'abd'][number + 1] AS s, if(number = 0, -0.0, 0.0) AS g FROM numbers(2))
GROUP BY g, s WITH CLUSTER 1;
