-- Tags: no-fasttest

-- A transform entry is matched against the value the way `=` matches it, including
-- when the entry is a shorter String cast into a wider FixedString, or the other way
-- round. CASE with an expression is built on transform and has to agree.

SELECT
    toFixedString('ab', 4) = 'ab'                               AS eq,
    transform(toFixedString('ab', 4), ['ab'], [1], 0)           AS padded_entry,
    transform('ab', [toFixedString('ab', 4)], [1], 0)           AS padded_value,
    transform(toFixedString('abcd', 4), ['abcd'], [1], 0)       AS exact_width
FORMAT TSV;

-- The same with a column, not a constant.
SELECT
    s = 'ab'                                        AS eq,
    CASE s WHEN 'ab' THEN 1 ELSE 0 END              AS case_when,
    transform(s, ['ab'], [1], 0)                    AS padded_entry
FROM (SELECT arrayJoin([toFixedString('ab', 4), toFixedString('xy', 4)]) AS s)
ORDER BY s
FORMAT TSV;

-- A FixedString entry list against a String value.
SELECT
    CASE 'ab' WHEN toFixedString('ab', 4) THEN 1 ELSE 0 END     AS case_when,
    transform('ab', [toFixedString('ab', 4)], [1], 0)           AS padded_entry
FORMAT TSV;

-- An entry that the cast cannot represent is still rejected by the cast itself.
SELECT transform(toFixedString('ab', 4), ['abcdef'], [1], 0); -- { serverError TOO_LARGE_STRING_SIZE }

-- Other types are untouched, including the Enum membership checks.
SELECT
    transform(toUInt8(5), [5], [1], 0)                                      AS number,
    transform(CAST('a' AS Enum8('a' = 1, 'b' = 2)), ['a'], [1], 0)          AS enum_by_name,
    transform(CAST('a' AS Enum8('a' = 1, 'b' = 2)), [toUInt8(9)], [1], 0)   AS enum_not_member,
    transform(CAST('a' AS Enum8('a' = 1, 'b' = 2)), [toUInt8(1)], [1], 0)   AS enum_by_value
FORMAT TSV;
