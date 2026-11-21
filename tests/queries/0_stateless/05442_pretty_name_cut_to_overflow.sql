-- `output_format_pretty_max_column_name_width_cut_to` near the `UInt64` maximum used to overflow `cut_to + hysteresis`
-- in `truncateName`: short names were reported as that wide, which broke `Pretty*` and `Vertical` output.

SET output_format_pretty_color = 0;

SELECT 'hello world' FORMAT PrettyCompact SETTINGS output_format_pretty_max_column_name_width_cut_to = 18446744073709551615;
SELECT 1 AS x, 'hello world' FORMAT Vertical SETTINGS output_format_pretty_max_column_name_width_cut_to = 18446744073709551615;
SELECT 1 AS x, 'hello world' FORMAT Vertical SETTINGS output_format_pretty_max_column_name_width_cut_to = 18446744073709551612;
SELECT tuple(1, 'a')::Tuple(`first element` UInt8, `second element` String) AS `t t` FORMAT PrettyCompact SETTINGS output_format_pretty_max_column_name_width_cut_to = 18446744073709551615;
SELECT 1 AS `a long name that gets cut` FORMAT PrettyCompact SETTINGS output_format_pretty_max_column_name_width_cut_to = 8, output_format_pretty_max_column_name_width_min_chars_to_cut = 4;
