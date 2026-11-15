SET allow_suspicious_variant_types = 1;

SELECT
    toTypeName(nullIf(CAST(toInt32(1) AS Variant(Int32, Int64)), toInt32(2))),
    nullIf(CAST(toInt32(1) AS Variant(Int32, Int64)), toInt32(1)),
    nullIf(CAST(toInt32(1) AS Variant(Int32, Int64)), toInt32(2));

SELECT
    toTypeName(nullIf(CAST(toInt32(1) AS Dynamic), toInt32(2))),
    nullIf(CAST(toInt32(1) AS Dynamic), toInt32(1)),
    nullIf(CAST(toInt32(1) AS Dynamic), toInt32(2));

SELECT toTypeName(nullIf(toInt32(1), toInt32(2))), nullIf(toInt32(1), toInt32(2));

SELECT count(r), countIf(isNotNull(r)), uniqExact(r), groupArray(r)
FROM
(
    SELECT nullIf(CAST(number AS Variant(UInt64, String)), number) AS r
    FROM numbers(3)
);

SELECT count(r), countIf(isNotNull(r)), uniqExact(r), groupArray(r)
FROM
(
    SELECT nullIf(CAST(number AS Dynamic), number) AS r
    FROM numbers(3)
);

-- Standalone `uniqExact` must not bypass internal-NULL filtering through either optimization.
SELECT uniqExact(nullIf(CAST(number AS Dynamic), number))
FROM numbers(3)
SETTINGS optimize_uniq_exact_radix_partitioning = 1;

SELECT uniqExact(nullIf(CAST(number AS Variant(UInt64, String)), number))
FROM numbers(3)
SETTINGS optimize_uniq_exact_radix_partitioning = 1;

DROP TABLE IF EXISTS squality_internal_null_uniq;
CREATE TABLE squality_internal_null_uniq (d Dynamic, v Variant(UInt64, String)) ENGINE = Memory;
INSERT INTO squality_internal_null_uniq VALUES (NULL, NULL), (1, 1), (NULL, NULL), (3, 3);

SELECT uniqExact(d), uniqExact(v)
FROM squality_internal_null_uniq
SETTINGS optimize_uniq_exact_radix_partitioning = 1, count_distinct_optimization = 0;

SELECT uniqExact(d), uniqExact(v)
FROM squality_internal_null_uniq
SETTINGS optimize_uniq_exact_radix_partitioning = 0, count_distinct_optimization = 1;

DROP TABLE squality_internal_null_uniq;

-- The transparent adapter must preserve existing nullable-combinator state layouts and types.
SELECT finalizeAggregation(CAST(
    unhex('0112000900000000000000040000000000000000'),
    'AggregateFunction(groupArrayIf, Dynamic, Nullable(UInt8))'));

SELECT groupArrayIf(number::Dynamic, NULL), groupArrayIf(CAST(number AS Variant(UInt64, String)), NULL)
FROM numbers(1);

SELECT toTypeName(countIfState(number::Dynamic, toNullable(toUInt8(1)))) FROM numbers(1);

-- Its state identity is the nested function's normalized identity.
SELECT finalizeAggregation(CAST(countState(number::Dynamic), 'AggregateFunction(count)')) FROM numbers(3);
SELECT finalizeAggregation(CAST(countState(CAST(number AS Variant(UInt64, String))), 'AggregateFunction(count)')) FROM numbers(3);
