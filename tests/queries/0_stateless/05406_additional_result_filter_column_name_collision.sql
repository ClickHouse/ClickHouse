-- A computed `additional_result_filter` must not be confused with a result column of the same name.

SELECT number AS a, number + 1 AS b, toUInt8(1) AS `greater(a, b)`
FROM numbers(3)
SETTINGS additional_result_filter = 'a > b'
FORMAT TSVWithNames;

SELECT number AS a, number + 1 AS b, toUInt8(0) AS `greater(b, a)`
FROM numbers(3)
SETTINGS additional_result_filter = 'b > a'
FORMAT TSVWithNames;

-- The filter is a result column itself.
SELECT number AS a, number % 2 AS c
FROM numbers(3)
SETTINGS additional_result_filter = 'c'
FORMAT TSVWithNames;
