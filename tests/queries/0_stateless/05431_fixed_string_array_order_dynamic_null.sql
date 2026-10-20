-- Arrays with `FixedString` elements against arrays of another element type are compared element by element.
-- A NULL element of a tuple, also of a `Dynamic` field or an untyped `NULL`, is equal only to another NULL
-- and sorts after every non-NULL value.

SELECT [tuple(toFixedString('a', 2), CAST(1 AS Dynamic))] < [tuple('b', CAST(1 AS Dynamic))];
SELECT [tuple(toFixedString('b', 2), CAST(1 AS Dynamic))] < [tuple('a', CAST(1 AS Dynamic))];
SELECT [tuple(toFixedString('a', 2), CAST(1 AS Dynamic))] <= [tuple('a', CAST(1 AS Dynamic))];
SELECT [tuple(toFixedString('a', 2), CAST(NULL AS Dynamic))] < [tuple('a', CAST(1 AS Dynamic))];
SELECT [tuple(toFixedString('a', 2), CAST(1 AS Dynamic))] < [tuple('a', CAST(NULL AS Dynamic))];
SELECT [tuple(toFixedString('a', 2), CAST(NULL AS Dynamic))] >= [tuple('a', CAST(NULL AS Dynamic))];

SELECT [tuple(toFixedString('a', 2), NULL)] <= [tuple('a', NULL)];
SELECT [tuple(toFixedString('a', 2), NULL)] < [tuple('a', NULL)];
SELECT [tuple(toFixedString('a', 2), 1)] < [tuple('a', NULL)];
SELECT [tuple(toFixedString('a', 2), NULL)] > [tuple('a', 1)];
SELECT [tuple(toFixedString('a', 2), NULL)] < [tuple('b', NULL)];
