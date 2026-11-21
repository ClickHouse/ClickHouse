SET output_format_pretty_color = 0;
SET output_format_pretty_row_numbers = 0;
SET output_format_pretty_display_footer_column_names = 0;
SET output_format_pretty_squash_consecutive_ms = 0;
SET output_format_pretty_max_column_name_width_min_chars_to_cut = 4;
SET output_format_pretty_max_column_name_width_cut_to = 16;

SELECT 'XXX' AS "一二三四五六七八九十一", 'XXX' AS "ABC" FORMAT Vertical;
SELECT 'XXX' AS "一二三四五六七八九十一", 'XXX' AS "ABC" FORMAT PrettyCompact;

SET output_format_pretty_max_column_name_width_cut_to = 15;
SELECT 'XXX' AS "一二三四五六七八九十一", 'XXX' AS "ABC" FORMAT Vertical;

SET output_format_pretty_max_column_name_width_cut_to = 16;
SELECT 'XXX' AS "ab一二三四五六七八九十", 'XXX' AS "ABC" FORMAT Vertical;

-- The truncated name is one cell narrower than the 16-cell ASCII name.
SELECT 'XXX' AS "a一二三四五六七八九十b", 'XXX' AS "ABCDEFGHIJKLMNOP" FORMAT Vertical;

SET output_format_pretty_named_tuples_as_subcolumns = 1;

-- Widen the subcolumns to fit the truncated group header.
SELECT CAST(('XXX', 'YYY'), 'Tuple(a String, b String)') AS "一二三四五六七八九十一" FORMAT PrettyCompact;

-- Recalculating widths must keep the 21-cell names truncated.
SELECT CAST(('XXX', 'YYY'), 'Tuple(a String, b String)') AS "一一一一a一一一一一一" FORMAT PrettyCompact;
SELECT CAST(('XXX', 'YYY'), 'Tuple(a String, b String)') AS "一二三四五六七八九十一", 'XXX' AS "一一一一a一一一一一一" FORMAT PrettyCompact;

-- A raw tab changes width when moved after the prefix and filler.
SET output_format_pretty_display_control_characters = 0;

SELECT 'XXX' AS "abcdefghijklmno\tabcde", 'XXX' AS "ABCDEFGHIJKLMNOP" FORMAT Vertical;
SELECT CAST(('XXX', 'YYY'), 'Tuple(a String, b String)') AS "abcdefghijklmno\tabcde" FORMAT PrettyCompact;
