-- A UTF-8 BOM before a header that is detected automatically is not a part of the first column name.

SELECT * FROM format(CSV, '\xEF\xBB\xBFa,b\n1,2\n') FORMAT TSVWithNames;
SELECT * FROM format(CSV, '\xEF\xBB\xBF"name","age"\n"bob",3\n') FORMAT TSVWithNames;
SELECT * FROM format(CSV, '\xEF\xBB\xBF"a,b",c\n1,2\n') FORMAT TSVWithNames;
SELECT * FROM format(TSV, '\xEF\xBB\xBFa\tb\n1\t2\n') FORMAT TSVWithNames;
SELECT * FROM format(TSV, '\xEF\xBB\xBFname\tage\nbob\t3\n') FORMAT TSVWithNames;
SELECT * FROM format(CustomSeparated, '\xEF\xBB\xBFa\tb\n1\t2\n') FORMAT TSVWithNames;
SELECT * FROM format(CustomSeparated, '\xEF\xBB\xBFname\tage\nbob\t3\n') FORMAT TSVWithNames;
SELECT * FROM format(CustomSeparated, '\xEF\xBB\xBF"a"\t"b"\n1\t2\n') SETTINGS format_custom_escaping_rule = 'JSON' FORMAT TSVWithNames;
SELECT * FROM format(CustomSeparated, '\xEF\xBB\xBFprefix:a\tb\n1\t2\n') SETTINGS format_custom_result_before_delimiter = 'prefix:' FORMAT TSVWithNames;

-- The header is also detected with an explicit structure.
SELECT * FROM format(CSV, 'name String, age UInt8', '\xEF\xBB\xBFname,age\nbob,3\n');

-- The format is detected from the file name.
INSERT INTO FUNCTION file(currentDatabase() || '_05407.csv', RawBLOB) SELECT '\xEF\xBB\xBFa,b\n1,2\n' SETTINGS engine_file_truncate_on_insert = 1;
SELECT * FROM file(currentDatabase() || '_05407.csv') FORMAT TSVWithNames;

-- With header detection, the BOM is skipped even if the first row is data.
SELECT hex(c1), c2 FROM format(CSV, '\xEF\xBB\xBFbob,3\n');
SELECT hex(x), y FROM format(CSV, 'x String, y UInt8', '\xEF\xBB\xBFbob,3\n');

-- Without header detection, a BOM before a `String` value is kept.
SELECT hex(c1), c2 FROM format(CSV, '\xEF\xBB\xBFbob,3\n') SETTINGS input_format_csv_detect_header = 0;
SELECT hex(x), y FROM format(CSV, 'x String, y UInt8', '\xEF\xBB\xBFbob,3\n') SETTINGS input_format_csv_detect_header = 0;
