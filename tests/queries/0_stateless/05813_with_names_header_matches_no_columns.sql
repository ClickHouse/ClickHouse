-- https://github.com/ClickHouse/ClickHouse/issues/47516
-- If no name in the header is a column of the table, the data is most likely parsed with wrong
-- settings, so it is an error instead of rows with only default values.

SET input_format_skip_unknown_fields = 1;

-- The file uses ';', but the delimiter is ',': the header is a single unknown column 'name;city'.
SELECT * FROM format(CSVWithNames, 'name String, city String', 'name;city\npablo;london\nraul;new york\n'); -- { serverError INCORRECT_DATA }
SELECT * FROM format(TSVWithNames, 'name String, city String', 'name;city\npablo;london\n'); -- { serverError INCORRECT_DATA }
SELECT * FROM format(CSVWithNamesAndTypes, 'name String, city String', 'name;city\nString;String\npablo;london\n'); -- { serverError INCORRECT_DATA }

-- With the right delimiter.
SELECT * FROM format(CSVWithNames, 'name String, city String', 'name;city\npablo;london\nraul;new york\n') SETTINGS format_csv_delimiter = ';';

-- Some columns of the header are known: the unknown ones are skipped and the missing ones are defaults.
SELECT * FROM format(CSVWithNames, 'name String, city String', 'name,country\npablo,uk\n');

-- Empty input has no header at all.
SELECT * FROM format(CSVWithNames, 'name String, city String', '');
SELECT * FROM format(TSVWithNames, 'name String, city String', '');

-- The previous behavior.
SELECT * FROM format(CSVWithNames, 'name String, city String', 'name;city\npablo;london\n') SETTINGS input_format_with_names_throw_if_no_column_matches = 0;

-- Reading the columns by their positions does not use the names.
SELECT * FROM format(CSVWithNames, 'name String', 'name;city\npablo;london\n') SETTINGS input_format_with_names_use_header = 0;
