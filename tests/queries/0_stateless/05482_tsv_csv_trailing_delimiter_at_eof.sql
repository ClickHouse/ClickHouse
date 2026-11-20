-- With `empty_as_default` and a variable number of columns, an empty field at the end of a row must be read as default,
-- even if it is not the last column of the schema, with or without a final newline.
-- https://github.com/ClickHouse/ClickHouse/issues/91776

SELECT 'TSV';
SELECT * FROM format(TSV, 'c1 String, c2 Int64, c3 String', 'a\t') SETTINGS input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_empty_as_default = 1;
SELECT * FROM format(TSV, 'c1 String, c2 Int64, c3 String', 'a\t\n') SETTINGS input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_empty_as_default = 1;
SELECT * FROM format(TSV, 'c1 String, c2 Float64, c3 String', 'a\t') SETTINGS input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_empty_as_default = 1;
SELECT * FROM format(TSV, 'c1 String, c2 Float64, c3 String', 'a\t\n') SETTINGS input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_empty_as_default = 1;
SELECT * FROM format(TSV, 'c1 String, c2 Date, c3 String', 'a\t') SETTINGS input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_empty_as_default = 1;
SELECT * FROM format(TSV, 'c1 String, c2 Date, c3 String', 'a\t\n') SETTINGS input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_empty_as_default = 1;
SELECT * FROM format(TSV, 'c1 String, c2 Nullable(Int64), c3 String', 'a\t') SETTINGS input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_empty_as_default = 1;
SELECT * FROM format(TSV, 'c1 String, c2 Nullable(Int64), c3 String', 'a\t\n') SETTINGS input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_empty_as_default = 1;
SELECT * FROM format(TSV, 'c1 String, c2 Array(UInt8), c3 String', 'a\t') SETTINGS input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_empty_as_default = 1;
SELECT * FROM format(TSV, 'c1 String, c2 Array(UInt8), c3 String', 'a\t\n') SETTINGS input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_empty_as_default = 1;
SELECT * FROM format(TSV, 'c1 String, c2 Int64, c3 Date, c4 Float64', 'a\t1\nb\t') SETTINGS input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_empty_as_default = 1;
SELECT * FROM format(TSVWithNames, 'c1 String, c2 Int64, c3 String', 'c1\tc2\tc3\na\t') SETTINGS input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_empty_as_default = 1;

SELECT 'CSV';
SELECT * FROM format(CSV, 'c1 String, c2 Int64, c3 String', 'a,') SETTINGS input_format_csv_allow_variable_number_of_columns = 1, input_format_csv_empty_as_default = 1;
SELECT * FROM format(CSV, 'c1 String, c2 Int64, c3 String', 'a,\n') SETTINGS input_format_csv_allow_variable_number_of_columns = 1, input_format_csv_empty_as_default = 1;
SELECT * FROM format(CSV, 'c1 String, c2 Float64, c3 String', 'a,') SETTINGS input_format_csv_allow_variable_number_of_columns = 1, input_format_csv_empty_as_default = 1;
SELECT * FROM format(CSV, 'c1 String, c2 Float64, c3 String', 'a,\n') SETTINGS input_format_csv_allow_variable_number_of_columns = 1, input_format_csv_empty_as_default = 1;
SELECT * FROM format(CSV, 'c1 String, c2 Date, c3 String', 'a,') SETTINGS input_format_csv_allow_variable_number_of_columns = 1, input_format_csv_empty_as_default = 1;
SELECT * FROM format(CSV, 'c1 String, c2 Date, c3 String', 'a,\n') SETTINGS input_format_csv_allow_variable_number_of_columns = 1, input_format_csv_empty_as_default = 1;
SELECT * FROM format(CSV, 'c1 String, c2 Nullable(Int64), c3 String', 'a,') SETTINGS input_format_csv_allow_variable_number_of_columns = 1, input_format_csv_empty_as_default = 1;
SELECT * FROM format(CSV, 'c1 String, c2 Nullable(Int64), c3 String', 'a,\n') SETTINGS input_format_csv_allow_variable_number_of_columns = 1, input_format_csv_empty_as_default = 1;
SELECT * FROM format(CSV, 'c1 String, c2 Array(UInt8), c3 String', 'a,') SETTINGS input_format_csv_allow_variable_number_of_columns = 1, input_format_csv_empty_as_default = 1;
SELECT * FROM format(CSV, 'c1 String, c2 Array(UInt8), c3 String', 'a,\n') SETTINGS input_format_csv_allow_variable_number_of_columns = 1, input_format_csv_empty_as_default = 1;
SELECT * FROM format(CSV, 'c1 String, c2 Int64, c3 Date, c4 Float64', 'a,1\nb,') SETTINGS input_format_csv_allow_variable_number_of_columns = 1, input_format_csv_empty_as_default = 1;
SELECT * FROM format(CSVWithNames, 'c1 String, c2 Int64, c3 String', 'c1,c2,c3\na,') SETTINGS input_format_csv_allow_variable_number_of_columns = 1, input_format_csv_empty_as_default = 1;

-- The original report: rows end with a delimiter, the last one without a final newline.
SELECT * FROM format(TSV, 'c1 String, c2 String, c3 String, c4 UInt64, c5 UInt64, c6 String, c7 String', 'REQUEST\t\tHome\t1765188115675\t1765188115886\tOK\t\nREQUEST\t\tHome\t') SETTINGS input_format_tsv_allow_variable_number_of_columns = 1, input_format_tsv_empty_as_default = 1;
