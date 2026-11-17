-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/132103
-- transform() and CASE-with-expression must match a FixedString input against
-- a shorter String/FixedString entry, consistent with `=` semantics.

-- Core report: FixedString input, shorter String entry
SELECT transform(toFixedString('ab', 4), ['ab'], [1], 0);
SELECT transform(toFixedString('abc', 5), ['abc'], [1], 0);

-- Reverse direction: String input, FixedString entry
SELECT transform('ab', [toFixedString('ab', 4)], [1], 0);

-- CASE with expression routes through transform
SELECT CASE toFixedString('ab', 4) WHEN 'ab' THEN 1 ELSE 0 END;
SELECT CASE 'ab' WHEN toFixedString('ab', 4) THEN 1 ELSE 0 END;

-- Entries with embedded NULs, and FixedString entries of a different width
SELECT transform(toFixedString('a\0b', 4), ['a\0b'], [1], 0);
SELECT transform(toFixedString('ab', 8), [toFixedString('ab', 4)], [1], 0);

-- Duplicate entries that normalize to the same value: the first one wins,
-- consistent with existing duplicate-key behavior for String and numeric keys
SELECT transform(toFixedString('ab', 4), ['ab', toFixedString('ab', 4)], [1, 2], 0);
SELECT transform(toFixedString('ab', 4), [toFixedString('ab', 4), 'ab'], [1, 2], 0);

-- Non-matching values still fall through to the default
SELECT transform(toFixedString('ac', 4), ['ab'], [1], 0);
SELECT transform(toFixedString('abcd', 4), ['ab'], [1], 0);

-- Entries longer than the FixedString width still throw
SELECT transform(toFixedString('ab', 2), ['abc'], [1], 0); -- { serverError TOO_LARGE_STRING_SIZE }

-- The numeric guard that the dropped check exists for must keep working:
-- fractional entries whose cast changes the value stay rejected, exact ones stay accepted
SELECT transform(toUInt8(5), [5.9], [1], 0);
SELECT transform(toUInt8(5), [5.0], [1], 0);

-- Enum handling untouched
SELECT transform(CAST('a' AS Enum8('a' = 1, 'b' = 2)), ['a', 'b'], [10, 20], 0);

-- Nullable input: successful padded conversions are covered by the fix
SELECT transform(CAST('ab' AS Nullable(FixedString(4))), ['ab'], [1], 0);
SELECT CASE CAST('ab' AS Nullable(FixedString(4))) WHEN 'ab' THEN 1 ELSE 0 END;

-- Nullable target: a failed cast becomes NULL and must NOT be admitted as a key
SELECT transform(CAST(NULL AS Nullable(FixedString(2))), ['abc'], [1], 0);
SELECT CASE CAST(NULL AS Nullable(FixedString(2))) WHEN 'abc' THEN 1 ELSE 0 END;
SELECT transform(CAST(NULL AS Nullable(FixedString(2))), [toFixedString('abc', 3)], [1], 0);
SELECT CASE CAST(NULL AS Nullable(FixedString(2))) WHEN toFixedString('abc', 3) THEN 1 ELSE 0 END;
SELECT transform(CAST('ab' AS Nullable(FixedString(2))), ['abc'], [1], 0);

-- A genuine NULL entry still matches, and a failed cast must not shadow it
SELECT transform(CAST(NULL AS Nullable(FixedString(2))), [NULL, 'abc'], [1, 2], 0);
SELECT transform(CAST(NULL AS Nullable(FixedString(2))), ['abc', NULL], [1, 2], 0);
SELECT transform(CAST(NULL AS Nullable(String)), [NULL], [1], 0);
