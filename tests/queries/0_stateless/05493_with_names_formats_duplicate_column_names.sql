-- Duplicate column names in the header of `*WithNames` formats are renamed to `name_1`, `name_2`, ... (like DuckDB and the `Arrow` format).

SELECT 'CSVWithNames';
DESC format(CSVWithNames, 'id,name,name,name\n1,a,b,c');
SELECT * FROM format(CSVWithNames, 'id,name,name,name\n1,a,b,c');

SELECT 'TSVWithNames';
DESC format(TSVWithNames, 'id\tname\tname\n1\ta\tb');
SELECT * FROM format(TSVWithNames, 'id\tname\tname\n1\ta\tb');

SELECT 'CSVWithNamesAndTypes';
DESC format(CSVWithNamesAndTypes, 'id,name,name\nUInt32,String,String\n1,a,b');
SELECT * FROM format(CSVWithNamesAndTypes, 'id,name,name\nUInt32,String,String\n1,a,b');

SELECT 'JSONCompactEachRowWithNames';
DESC format(JSONCompactEachRowWithNames, '["id","name","name"]\n[1,"a","b"]');
SELECT * FROM format(JSONCompactEachRowWithNames, '["id","name","name"]\n[1,"a","b"]');

SELECT 'header detected automatically';
DESC format(CSV, 'id,name,name\n1,a,b');

SELECT 'renamed name collides with a name from the header';
DESC format(CSVWithNames, 'a,a,a_1\n1,2,3');
SELECT * FROM format(CSVWithNames, 'a,a,a_1\n1,2,3');

SELECT 'names are case-sensitive';
DESC format(CSVWithNames, 'a,A,a\n1,2,3');

SELECT 'explicit structure, matched by name';
SELECT * FROM format(CSVWithNames, 'name_1 String, id UInt32, name String', 'id,name,name\n1,a,b');

SELECT 'header ignored';
SELECT * FROM format(CSVWithNames, 'x UInt32, y String, z String', 'id,name,name\n1,a,b') SETTINGS input_format_with_names_use_header = 0;
