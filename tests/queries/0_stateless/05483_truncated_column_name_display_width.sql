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
