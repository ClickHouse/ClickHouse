-- Reading a Time64 variant of a Variant column from CSV.
-- A CSV field of a Variant column is offered to the variant types in rank order; Time64
-- ranks above String, so the Time64 CSV reader decides what every field below becomes.
-- Expected behaviour: docs/reference/data-types/variant.mdx, section
-- "Reading Variant type from the data"; docs/reference/data-types/time64.mdx, sections
-- "Normalization", "Negative values" and "Text representation range".

SET enable_time_time64_type = 1;
SET enable_variant_type = 1;
-- Otherwise an empty CSV field is replaced by the column default and never reaches the parser.
SET input_format_csv_empty_as_default = 0;

-- 1, 2: a time in single or in double quotes, and 3: an unquoted time, are a Time64.
-- 4, 5: a field with data after the time is not a Time64, quoted or unquoted, and
-- 6: neither is a field without a time: all three are the String variant, as read by CSV.
-- 7: an empty field is the empty String, not a Time64 and not NULL.
-- 8: a leading minus is kept. 9: the components are normalized, not validated.
SELECT tag, variantType(v) AS variant, toString(v) AS value
FROM format(CSV, 'tag String, v Variant(Time64(3), String)', $$1_quoted_single,'12:34:56.789'
2_quoted_double,"01:02:03.004"
3_unquoted,23:59:59.999
4_quoted_trailing,"01:02:03.004x"
5_unquoted_trailing,12:34:56.789x
6_not_a_time,notatime
7_empty,
8_negative,-01:02:03.123
9_normalized,25:70:70.5
$$)
ORDER BY tag;

-- Without a String variant the fields of 4 to 6 have no variant that accepts them.
SELECT v FROM format(CSV, 'v Variant(Time64(3), Int32)', '"01:02:03.004x"'); -- { serverError INCORRECT_DATA }
SELECT v FROM format(CSV, 'v Variant(Time64(3), Int32)', '12:34:56.789x'); -- { serverError INCORRECT_DATA }
SELECT v FROM format(CSV, 'v Variant(Time64(3), Int32)', 'notatime'); -- { serverError INCORRECT_DATA }
