-- A UTF-8 BOM before a header that is detected automatically is not a part of the first column name.

SELECT * FROM format(CSV, '\xEF\xBB\xBFa,b\n1,2\n') FORMAT TSVWithNames;
SELECT * FROM format(CSV, '\xEF\xBB\xBF"name","age"\n"bob",3\n') FORMAT TSVWithNames;
SELECT * FROM format(TSV, '\xEF\xBB\xBFa\tb\n1\t2\n') FORMAT TSVWithNames;
SELECT * FROM format(TSV, '\xEF\xBB\xBFname\tage\nbob\t3\n') FORMAT TSVWithNames;
SELECT * FROM format(CustomSeparated, '\xEF\xBB\xBFa\tb\n1\t2\n') FORMAT TSVWithNames;
SELECT * FROM format(CustomSeparated, '\xEF\xBB\xBFname\tage\nbob\t3\n') FORMAT TSVWithNames;

-- The header is also detected with an explicit structure.
SELECT * FROM format(CSV, 'name String, age UInt8', '\xEF\xBB\xBFname,age\nbob,3\n');

-- The format is detected from the file name.
INSERT INTO FUNCTION file(currentDatabase() || '_05407.csv', RawBLOB) SELECT '\xEF\xBB\xBFa,b\n1,2\n' SETTINGS engine_file_truncate_on_insert = 1;
SELECT * FROM file(currentDatabase() || '_05407.csv') FORMAT TSVWithNames;

-- If the first row is not a header, a BOM before a `String` value is kept, as without header detection.
SELECT hex(c1), c2 FROM format(CSV, '\xEF\xBB\xBFbob,3\n');

-- A quoted name after a BOM can contain a delimiter.
SELECT * FROM format(CSV, '\xEF\xBB\xBF"a,b",c\n1,2\n') FORMAT TSVWithNames;
SELECT * FROM format(CSV, '`a,b` String, c UInt8', '\xEF\xBB\xBF"a,b",c\nx,2\n') FORMAT TSVWithNames;

-- A data row that is valid only with the BOM is parsed with the BOM, as without header detection.
SELECT hex(x), y FROM format(CSV, 'x String, y UInt8', '\xEF\xBB\xBF"abc"x,3\n');
SELECT hex(c1), c2 FROM format(CSV, '\xEF\xBB\xBF"abc"x,3\n');
SELECT hex(c1), c2 FROM format(CSV, '\xEF\xBB\xBF"abc"x,3\ny,4\n');

-- A quoted name after a BOM with the `JSON` escaping rule.
SELECT * FROM format(CustomSeparated, '\xEF\xBB\xBF"a"\t"b"\n1\t2\n') SETTINGS format_custom_escaping_rule = 'JSON' FORMAT TSVWithNames;

-- Reading the first row without the BOM does not change how the data is read if it is not a header.
SELECT hex(c1), c2, c3 FROM format(CustomSeparated, '\xEF\xBB\xBF"a,b",c\nx,y,z\n') SETTINGS format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = ',';
SELECT hex(x), y, z FROM format(CustomSeparated, 'x String, y String, z String', '\xEF\xBB\xBF"a,b",c\nx,y,z\n') SETTINGS format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = ',';
SELECT hex(c1), c2 FROM format(CustomSeparated, '\xEF\xBB\xBFbob\t3\n') SETTINGS format_custom_row_between_delimiter = '|';
SELECT hex(x), y FROM format(CustomSeparated, 'x String, y UInt8', '\xEF\xBB\xBFbob\t3\n') SETTINGS format_custom_row_between_delimiter = '|';

-- Only the first row without the BOM is checked to be a header: with the BOM, a quoted name is split into fields of other types.
SELECT * FROM format(CSV, '\xEF\xBB\xBF"a,2020,b",c\n1,2\n') FORMAT TSVWithNames;
SELECT * FROM format(CSV, '\xEF\xBB\xBF"a,b",c\n,\n1,2\n') FORMAT TSVWithNames;

-- A BOM before a nonempty result prefix.
SELECT * FROM format(CustomSeparated, '\xEF\xBB\xBFprefix:a\tb\n1\t2\n') SETTINGS format_custom_result_before_delimiter = 'prefix:' FORMAT TSVWithNames;
SELECT * FROM format(CustomSeparated, 'a String, b UInt8', '\xEF\xBB\xBFprefix:a\tb\n1\t2\n') SETTINGS format_custom_result_before_delimiter = 'prefix:' FORMAT TSVWithNames;
SELECT * FROM format(CustomSeparated, 'a String, b UInt8', '\xEF\xBB\xBFprefix:x\t2\n') SETTINGS format_custom_result_before_delimiter = 'prefix:' FORMAT TSVWithNames;

-- If the variant without the BOM has another number of fields than the following rows, the first row is data.
SELECT * FROM format(CSV, '\xEF\xBB\xBF"a,2020,b",c\n1,2,3,4\n') FORMAT TSVWithNames;
SELECT * FROM format(CSV, '\xEF\xBB\xBF"a,2020,b",c\n1,2,3,4\n') SETTINGS input_format_csv_allow_variable_number_of_columns = 1 FORMAT TSVWithNames;

-- A variable number of columns in the rows read as data during header detection.
SELECT * FROM format(CSV, '\xEF\xBB\xBF"a,2020,b",c\nx,y,z\n') SETTINGS input_format_csv_allow_variable_number_of_columns = 1 FORMAT TSVWithNames;
SELECT * FROM format(CustomSeparated, '\xEF\xBB\xBF"a,2020,b",c\nx,y,z\n') SETTINGS format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = ',', input_format_custom_allow_variable_number_of_columns = 1 FORMAT TSVWithNames;

-- The rows read during header detection are kept when the end of data is reached.
SELECT * FROM format(CSV, '\xEF\xBB\xBF"a,2020,b",c\nx,,z,w,lost\n') SETTINGS input_format_csv_allow_variable_number_of_columns = 1 FORMAT TSVWithNames;
SELECT * FROM format(CustomSeparated, '\xEF\xBB\xBF"a,2020,b",c\nx,,z,w,lost\n') SETTINGS format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = ',', input_format_custom_allow_variable_number_of_columns = 1 FORMAT TSVWithNames;

-- The row with type names must have the same number of fields as the variant without the BOM.
SELECT * FROM format(CSV, '\xEF\xBB\xBF"a,2020,b",c\nString,String,String,String\nx,2\n') SETTINGS input_format_csv_allow_variable_number_of_columns = 1 FORMAT TSVWithNames;
SELECT * FROM format(CustomSeparated, '\xEF\xBB\xBF"a,2020,b",c\nString,String,String,String\nx,2\n') SETTINGS format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = ',', input_format_custom_allow_variable_number_of_columns = 1 FORMAT TSVWithNames;
