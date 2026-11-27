-- A field that starts with `\r` not followed by `\n` is data, not an empty field at the end of the row.
-- https://github.com/ClickHouse/ClickHouse/issues/130596

SELECT 'CSV';
SELECT c1, hex(c2), c3 FROM format(CSV, 'c1 String, c2 String, c3 String', 'a,\rX,b\n') SETTINGS input_format_csv_empty_as_default = 1;
SELECT hex(c1), c2 FROM format(CSV, 'c1 String, c2 String', '\rX,b\n') SETTINGS input_format_csv_empty_as_default = 1;
SELECT hex(c1), c2 FROM format(CSV, 'c1 String, c2 String', '\rX,b\n') SETTINGS input_format_csv_empty_as_default = 1, input_format_csv_allow_variable_number_of_columns = 1;
SELECT c1, hex(c2) FROM format(CSV, 'c1 String, c2 String', 'a,\rX\n') SETTINGS input_format_csv_empty_as_default = 1;
SELECT c1, hex(c2), c3 FROM format(CSV, 'c1 String, c2 String, c3 String', 'a,\rX,b\n') SETTINGS input_format_csv_empty_as_default = 1, input_format_csv_allow_variable_number_of_columns = 1;
SELECT c1, hex(c2), c3 FROM format(CSVWithNames, 'c1 String, c2 String, c3 String', 'c1,c2,c3\na,\rX,b\n') SETTINGS input_format_csv_empty_as_default = 1;
SELECT * FROM format(CSV, 'c1 String, c2 Int64, c3 String', 'a,\r\nb,1,c\r\n') SETTINGS input_format_csv_empty_as_default = 1, input_format_csv_allow_variable_number_of_columns = 1;
SELECT * FROM format(CSV, 'c1 String, c2 Int64, c3 String', 'a,\rb,1,c\r') SETTINGS input_format_csv_empty_as_default = 1, input_format_csv_allow_variable_number_of_columns = 1, input_format_csv_allow_cr_end_of_line = 1;
SELECT c1, hex(c2) FROM format(CSV, 'c1 String, c2 String', 'a,\r') SETTINGS input_format_csv_empty_as_default = 1; -- { serverError INCORRECT_DATA }

SELECT 'TSV';
SELECT c1, hex(c2), c3 FROM format(TSV, 'c1 String, c2 String, c3 String', 'a\t\rX\tb\r\n') SETTINGS input_format_tsv_empty_as_default = 1, input_format_tsv_crlf_end_of_line = 1;
SELECT c1, hex(c2) FROM format(TSV, 'c1 String, c2 String', 'a\t\rX\r\n') SETTINGS input_format_tsv_empty_as_default = 1, input_format_tsv_crlf_end_of_line = 1;
SELECT hex(c1), c2 FROM format(TSV, 'c1 String, c2 String', '\rX\tb\r\n') SETTINGS input_format_tsv_empty_as_default = 1, input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_crlf_end_of_line = 1;
SELECT * FROM format(TSV, 'c1 String, c2 Int64, c3 String', 'a\t\r\nb\t1\tc\r\n') SETTINGS input_format_tsv_empty_as_default = 1, input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_crlf_end_of_line = 1;
SELECT c1, hex(c2) FROM format(TSV, 'c1 String, c2 String', 'a\t\r') SETTINGS input_format_tsv_empty_as_default = 1, input_format_tsv_crlf_end_of_line = 1; -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
