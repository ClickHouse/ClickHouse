-- `array_length(arr, dimension)` of PostgreSQL: the length of the array, and `NULL` for an empty array
-- and for a dimension the array does not have.
SELECT array_length([1, 2, 3], 1), ARRAY_LENGTH(['a'], 1), array_length([[1, 2], [3, 4]], 1);
SELECT array_length([], 1), array_length(emptyArrayString(), 1);
SELECT array_length([1, 2, 3], 2), array_length([1, 2, 3], 0), array_length([1, 2, 3], -1), array_length([[1, 2], [3, 4]], 3);
SELECT array_length(arr, 1) FROM (SELECT arrayJoin([[1, 2], [], [3]]) AS arr);
SELECT array_length([1, 2], NULL), array_length([1, 2], toNullable(1)), toTypeName(array_length([1], 1));

-- The nested arrays can have different sizes, so another dimension of a nested array has no single length.
SELECT array_length([[1, 2], [3, 4]], 2); -- { serverError NOT_IMPLEMENTED }
SELECT array_length('a', 1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT array_length([1]); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
