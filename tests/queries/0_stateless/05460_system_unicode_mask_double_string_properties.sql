-- Tags: no-fasttest
-- Tag no-fasttest: depends on the ICU library
--
-- The property columns of system.unicode documented in
-- docs/reference/system-tables/unicode.mdx: the mask property `general_category_mask`,
-- the double property `numeric_value`, and the string properties `age`,
-- `bidi_mirroring_glyph`, `case_folding`, `simple_case_folding`,
-- `simple_lowercase_mapping`, `simple_titlecase_mapping`, `titlecase_mapping` and
-- `bidi_paired_bracket`.

-- Properties of the whole table that do not depend on the ICU version:
--   `general_category_mask` is the single bit `1 << general_category`;
--   `age` is a Unicode version string;
--   the four "simple" one-to-one mappings and both bidi columns are always exactly
--   one code point, while the full `case_folding` is at most three code points.
-- The full mappings may be longer than one code point by definition, so no length
-- is required of `titlecase_mapping` here.
-- Every counter below counts violations, so every counter must be 0.
SELECT
    countIf(general_category_mask != bitShiftLeft(toInt64(1), toInt16(general_category))) AS mask_is_not_one_bit,
    countIf(NOT match(age, '^[0-9]+\\.[0-9]+$')) AS age_is_not_a_version,
    countIf(lengthUTF8(bidi_mirroring_glyph) != 1) AS mirroring_glyph_is_not_one_char,
    countIf(lengthUTF8(bidi_paired_bracket) != 1) AS paired_bracket_is_not_one_char,
    countIf(lengthUTF8(simple_case_folding) != 1) AS simple_case_folding_is_not_one_char,
    countIf(lengthUTF8(simple_lowercase_mapping) != 1) AS simple_lowercase_is_not_one_char,
    countIf(lengthUTF8(simple_titlecase_mapping) != 1) AS simple_titlecase_is_not_one_char,
    countIf(lengthUTF8(case_folding) > 3) AS case_folding_is_too_long
FROM system.unicode
FORMAT Vertical;

-- The same columns for four long-stable code points:
--   U+0028 '(' is mirrored and is an opening paired bracket;
--   U+0031 '1' has numeric_type 'Decimal';
--   U+00BD '½' has numeric_type 'Numeric';
--   U+1E9E 'ẞ' folds to the two-character 'ss', while its simple folding is 'ß'.
SELECT notation, general_category, general_category_mask, numeric_type, age
FROM system.unicode
WHERE code_point_value IN (0x28, 0x31, 0xBD, 0x1E9E)
ORDER BY code_point_value;

-- `numeric_value` for the two code points whose Unicode numeric value is unambiguous:
-- U+00BD is `numeric_type = 'Numeric'` with the value 0.5, and U+0028 '(' is not a
-- numeric character at all. Decimal digits are left out on purpose: their Unicode
-- numeric value is the digit itself (UnicodeData.txt), so this test pins none of them.
SELECT notation, numeric_type, numeric_value
FROM system.unicode
WHERE code_point_value IN (0x28, 0xBD)
ORDER BY code_point_value;

SELECT notation, bidi_mirroring_glyph, bidi_paired_bracket, case_folding, simple_case_folding,
       simple_lowercase_mapping, simple_titlecase_mapping, titlecase_mapping
FROM system.unicode
WHERE code_point_value IN (0x28, 0x31, 0xBD, 0x1E9E)
ORDER BY code_point_value;
