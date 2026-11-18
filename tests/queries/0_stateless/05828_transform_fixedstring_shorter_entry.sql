-- A String entry shorter than the FixedString width must still be indexed by transform().
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/132103
SELECT
    toFixedString('ab', 4) = 'ab' AS eq_control,
    transform(toFixedString('ab', 4), ['ab'], [1], 0) AS transform_fixedstring_shorter,
    transform('ab', [toFixedString('ab', 4)], [1], 0) AS transform_string_from_fixedstring,
    CASE toFixedString('ab', 4) WHEN 'ab' THEN 1 ELSE 0 END AS case_when,
    transform(toFixedString('a', 3), ['a'], [1], 0) AS transform_single_char,
    transform(toFixedString('abcd', 4), ['abcd'], [1], 0) AS exact_width_control,
    transform(toUInt8(5), [5], [1], 0) AS numeric_control,
    transform(CAST('RED' AS Enum8('RED' = 1, 'GREEN' = 2)), ['RED'], [1], 0) AS enum_control;
