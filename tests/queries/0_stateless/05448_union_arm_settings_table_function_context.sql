-- The `SETTINGS` clause of a parenthesized operand of a set operation applies only to that operand,
-- and a table function in it must be executed (and its structure inferred) with these settings.

-- The outermost query: the first operand returns no rows, to keep the order of the result.
SELECT CAST(99, 'Nullable(UInt64)') AS x WHERE 0
UNION ALL
(
    SELECT x
    FROM view(SELECT sum(number) AS x FROM numbers(0))
    SETTINGS aggregate_functions_null_for_empty = 1
);

SELECT '--';

SELECT * FROM
(
    SELECT 'a', toTypeName(x) FROM view(SELECT sum(number) AS x FROM numbers(0))
    UNION ALL
    (SELECT 'b', toTypeName(x) FROM view(SELECT sum(number) AS x FROM numbers(0)) SETTINGS aggregate_functions_null_for_empty = 1)
)
ORDER BY 1;

SELECT '--';

SELECT * FROM
(
    SELECT 1::UInt8 AS x
    UNION ALL
    (SELECT x FROM view(SELECT 'a' AS x UNION ALL SELECT 1) SETTINGS use_variant_as_common_type = 1)
)
ORDER BY toString(x);

SELECT '--';

-- A compound parenthesized operand.
SELECT * FROM
(
    SELECT CAST(99, 'Nullable(UInt64)') AS x
    UNION ALL
    (
        SELECT x FROM view(SELECT sum(number) AS x FROM numbers(0))
        UNION ALL
        SELECT 5
        SETTINGS aggregate_functions_null_for_empty = 1
    )
)
ORDER BY x;

SELECT '--';

-- The trailing clause of the whole set operation still applies to every operand.
SELECT * FROM
(
    SELECT 'a', toTypeName(x) FROM view(SELECT sum(number) AS x FROM numbers(0))
    UNION ALL
    SELECT 'b', toTypeName(x) FROM view(SELECT sum(number) AS x FROM numbers(0))
    SETTINGS aggregate_functions_null_for_empty = 1
)
ORDER BY 1;
