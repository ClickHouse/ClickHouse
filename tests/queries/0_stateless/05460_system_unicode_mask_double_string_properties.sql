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
--   `numeric_value` is filled only for `numeric_type = 'Numeric'` and stays at the
--   default 0 for every other numeric type;
--   `age` is a Unicode version string;
--   the four "simple" one-to-one mappings and both bidi columns are always exactly
--   one code point, while the full `case_folding` is at most three code points.
-- Every counter below counts violations, so every counter must be 0.
SELECT
    countIf(general_category_mask != bitShiftLeft(toInt64(1), toInt16(general_category))) AS mask_is_not_one_bit,
    countIf(numeric_type != 'Numeric' AND numeric_value != 0) AS numeric_value_without_numeric_type,
    countIf(NOT match(age, '^[0-9]+\\.[0-9]+$')) AS age_is_not_a_version,
    countIf(lengthUTF8(bidi_mirroring_glyph) != 1) AS mirroring_glyph_is_not_one_char,
    countIf(lengthUTF8(bidi_paired_bracket) != 1) AS paired_bracket_is_not_one_char,
    countIf(lengthUTF8(simple_case_folding) != 1) AS simple_case_folding_is_not_one_char,
    countIf(lengthUTF8(simple_lowercase_mapping) != 1) AS simple_lowercase_is_not_one_char,
    countIf(lengthUTF8(simple_titlecase_mapping) != 1) AS simple_titlecase_is_not_one_char,
    countIf(lengthUTF8(titlecase_mapping) != 1) AS titlecase_is_not_one_char,
    countIf(lengthUTF8(case_folding) > 3) AS case_folding_is_too_long
FROM system.unicode
FORMAT Vertical;

-- The same columns for four long-stable code points:
--   U+0028 '(' is mirrored and is an opening paired bracket;
--   U+0031 '1' has numeric_type 'Decimal', so numeric_value stays 0;
--   U+00BD '½' has numeric_type 'Numeric', so numeric_value is 0.5;
--   U+1E9E 'ẞ' folds to the two-character 'ss', while its simple folding is 'ß'.
SELECT notation, general_category, general_category_mask, numeric_type, numeric_value, age
FROM system.unicode
WHERE code_point_value IN (0x28, 0x31, 0xBD, 0x1E9E)
ORDER BY code_point_value;

SELECT notation, bidi_mirroring_glyph, bidi_paired_bracket, case_folding, simple_case_folding,
       simple_lowercase_mapping, simple_titlecase_mapping, titlecase_mapping
FROM system.unicode
WHERE code_point_value IN (0x28, 0x31, 0xBD, 0x1E9E)
ORDER BY code_point_value;
