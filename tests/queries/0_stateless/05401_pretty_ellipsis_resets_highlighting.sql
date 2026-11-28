-- The ellipsis of a cut value must not inherit an escape sequence left open by the cut (issue #104834).

SET output_format_pretty_color = 1, output_format_pretty_highlight_trailing_spaces = 1, output_format_pretty_highlight_digit_groups = 1,
    output_format_pretty_max_value_width = 10, output_format_pretty_max_value_width_apply_for_single_value = 1,
    output_format_pretty_display_footer_column_names = 0, output_format_pretty_row_numbers = 0,
    output_format_pretty_fallback_to_vertical = 0, output_format_pretty_single_large_number_tip_threshold = 0,
    output_format_pretty_display_control_characters = 1;

-- The cut falls inside the highlighted trailing whitespace.
SELECT 'hello                    ' AS x FORMAT PrettyCompact;
SELECT 'hello                    ' AS x FORMAT Pretty;
SELECT 'hello                    ' AS x FORMAT PrettySpace;

-- The same, with the whitespace highlighted together with a Control Picture.
SELECT 'hello\r                   ' AS x FORMAT PrettyCompact;

-- The cut falls right after the underline of a digit group.
SELECT 12345678901234 AS n FORMAT PrettyCompact;
