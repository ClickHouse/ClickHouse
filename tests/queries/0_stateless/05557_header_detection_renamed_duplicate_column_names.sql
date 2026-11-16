-- Automatic header detection compares the names of the first row after renaming the duplicates,
-- the same way as the `*WithNames` formats do, so a structure with only a renamed name still detects the header.

SELECT 'CSV';
SELECT * FROM format(CSV, 'a_1 UInt32', 'a,a\n1,2') SETTINGS input_format_skip_unknown_fields = 1;
SELECT * FROM format(CSV, 'a_1 String', 'a,a\nfoo,bar') SETTINGS input_format_skip_unknown_fields = 1, input_format_csv_allow_variable_number_of_columns = 1;

SELECT 'TSV';
SELECT * FROM format(TSV, 'a_1 UInt32', 'a\ta\n1\t2') SETTINGS input_format_skip_unknown_fields = 1;
SELECT * FROM format(TSV, 'a_1 String', 'a\ta\nfoo\tbar') SETTINGS input_format_skip_unknown_fields = 1, input_format_tsv_allow_variable_number_of_columns = 1;

SELECT 'CustomSeparated';
SELECT * FROM format(CustomSeparated, 'a_1 UInt32', 'a\ta\n1\t2\n') SETTINGS input_format_skip_unknown_fields = 1, format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = '\t', format_custom_row_after_delimiter = '\n', input_format_custom_detect_header = 1;
