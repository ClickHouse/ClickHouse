-- The setting `input_format_with_names_rename_duplicate_columns` controls renaming of duplicate names in headers.
-- With it disabled, unknown fields with duplicate names are skipped as before, instead of being mapped to a renamed column.

SELECT 'renaming enabled (default)';
SELECT * FROM format(CSVWithNames, 'id UInt32, name_1 String', 'id,name,name\n1,a,b') SETTINGS input_format_skip_unknown_fields = 1;
SELECT * FROM format(TSVWithNames, 'id UInt32, name_1 String', 'id\tname\tname\n1\ta\tb') SETTINGS input_format_skip_unknown_fields = 1;

SELECT 'renaming disabled';
SELECT * FROM format(CSVWithNames, 'id UInt32, name_1 String', 'id,name,name\n1,a,b') SETTINGS input_format_skip_unknown_fields = 1, input_format_with_names_rename_duplicate_columns = 0;
SELECT * FROM format(TSVWithNames, 'id UInt32, name_1 String', 'id\tname\tname\n1\ta\tb') SETTINGS input_format_skip_unknown_fields = 1, input_format_with_names_rename_duplicate_columns = 0;
