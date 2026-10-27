-- A value width limit large enough for `limit * 4` to wrap around made the measured width of the values 0,
-- so the table borders were not aligned with the values.
SELECT 'hello world' AS x, 123456789 AS y FORMAT PrettyCompactMonoBlock SETTINGS output_format_pretty_max_value_width = 4611686018427387904, output_format_pretty_max_column_pad_width = 4611686018427387904, output_format_pretty_color = 0, output_format_pretty_single_large_number_tip_threshold = 0;
SELECT 'hello world' AS x, 123456789 AS y FORMAT PrettyCompactMonoBlock SETTINGS output_format_pretty_max_value_width = 18446744073709551615, output_format_pretty_max_column_pad_width = 18446744073709551615, output_format_pretty_color = 0, output_format_pretty_single_large_number_tip_threshold = 0;
