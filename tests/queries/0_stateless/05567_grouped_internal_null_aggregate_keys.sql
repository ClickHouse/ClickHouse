SELECT k, count(v), groupArray(v)
FROM
(
    SELECT
        toUInt8(number % 2) AS k,
        nullIf(number::Dynamic, number::Dynamic) AS v
    FROM numbers(4)
)
GROUP BY k
ORDER BY k;

SELECT k, count(v), groupArray(v)
FROM
(
    SELECT
        toUInt8(number % 2) AS k,
        nullIf(number::Variant(UInt64, String), number::Variant(UInt64, String)) AS v
    FROM numbers(4)
)
GROUP BY k
ORDER BY k;
