-- Regression test for https://github.com/ClickHouse/ClickHouse/pull/80263
-- The `LowCardinality` optimization of `if` / `multiIf` with constant string branches compares the branch
-- values only once the branches are known to be strings. Identical non-string constant branches, such as an
-- array with more than 1000000 elements, must not be materialized as `Field` during return type inference.

WITH arrayWithConstant(1000001, toUInt8(1)) AS a
SELECT length(if(number % 2 = 0, a, a)), length(multiIf(number % 2 = 0, a, number = 1, a, a))
FROM numbers(2)
SETTINGS optimize_if_transform_const_strings_to_lowcardinality = 0;

WITH arrayWithConstant(1000001, toUInt8(1)) AS a
SELECT length(if(number % 2 = 0, a, a)), length(multiIf(number % 2 = 0, a, number = 1, a, a))
FROM numbers(2)
SETTINGS optimize_if_transform_const_strings_to_lowcardinality = 1, optimize_if_transform_strings_to_enum = 0;

-- Identical constant string branches still keep plain `String`.
SELECT toTypeName(if(number % 2 = 0, 'a', 'a')), toTypeName(multiIf(number % 2 = 0, 'a', number = 1, 'a', 'a'))
FROM numbers(1)
SETTINGS optimize_if_transform_const_strings_to_lowcardinality = 1, optimize_if_transform_strings_to_enum = 0;

SELECT toTypeName(if(number % 2 = 0, 'a', 'b')), toTypeName(multiIf(number % 2 = 0, 'a', number = 1, 'b', 'c'))
FROM numbers(1)
SETTINGS optimize_if_transform_const_strings_to_lowcardinality = 1, optimize_if_transform_strings_to_enum = 0;
