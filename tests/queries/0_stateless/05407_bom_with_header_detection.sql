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
