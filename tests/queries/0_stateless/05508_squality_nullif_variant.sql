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
