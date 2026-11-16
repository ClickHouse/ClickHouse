-- Issue #131641: the CSV-family *WithNames input formats read a Tuple column back from the flattened header
-- (t.1, nt.a, nst.y.p) that their output formats write.
DROP TABLE IF EXISTS tr;
CREATE TABLE tr (id UInt64, name String, t Tuple(UInt64, String), nt Tuple(a UInt64, b String), nst Tuple(x UInt8, y Tuple(p UInt8, q String))) ENGINE = Memory;
INSERT INTO tr SELECT number, concat('n', toString(number)), (number * 10, concat('s', toString(number))), (number * 100, concat('t', toString(number))), (number, (number + 1, concat('q', toString(number)))) FROM numbers(3);

INSERT INTO FUNCTION file(currentDatabase() || '_05815.csv', 'CSVWithNames') SELECT * FROM tr SETTINGS engine_file_truncate_on_insert = 1;
SELECT * FROM file(currentDatabase() || '_05815.csv', 'CSVWithNames', 'id UInt64, name String, t Tuple(UInt64, String), nt Tuple(a UInt64, b String), nst Tuple(x UInt8, y Tuple(p UInt8, q String))') ORDER BY id;
SELECT * FROM file(currentDatabase() || '_05815.csv', 'CSVWithNames', 'id UInt64, name String, t Tuple(UInt64, String), nt Tuple(a UInt64, b String), nst Tuple(x UInt8, y Tuple(p UInt8, q String))') ORDER BY id SETTINGS input_format_skip_unknown_fields = 0;
SELECT * FROM file(currentDatabase() || '_05815.csv', 'CSV', 'id UInt64, name String, t Tuple(UInt64, String), nt Tuple(a UInt64, b String), nst Tuple(x UInt8, y Tuple(p UInt8, q String))') ORDER BY id;

INSERT INTO FUNCTION file(currentDatabase() || '_05815_types.csv', 'CSVWithNamesAndTypes') SELECT * FROM tr SETTINGS engine_file_truncate_on_insert = 1;
SELECT * FROM file(currentDatabase() || '_05815_types.csv', 'CSVWithNamesAndTypes', 'id UInt64, name String, t Tuple(UInt64, String), nt Tuple(a UInt64, b String), nst Tuple(x UInt8, y Tuple(p UInt8, q String))') ORDER BY id;
SELECT * FROM file(currentDatabase() || '_05815_types.csv', 'CSVWithNamesAndTypes', 'id UInt64, name String, t Tuple(UInt32, String), nt Tuple(a UInt64, b String), nst Tuple(x UInt8, y Tuple(p UInt8, q String))'); -- { serverError INCORRECT_DATA }

INSERT INTO FUNCTION file(currentDatabase() || '_05815_custom.csv', 'CustomSeparatedWithNames') SELECT * FROM tr SETTINGS engine_file_truncate_on_insert = 1, format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = ',';
SELECT * FROM file(currentDatabase() || '_05815_custom.csv', 'CustomSeparatedWithNames', 'id UInt64, name String, t Tuple(UInt64, String), nt Tuple(a UInt64, b String), nst Tuple(x UInt8, y Tuple(p UInt8, q String))') ORDER BY id SETTINGS format_custom_escaping_rule = 'CSV', format_custom_field_delimiter = ',';

SELECT * FROM file(currentDatabase() || '_05815.csv', 'CSVWithNames', 'ID UInt64, NAME String, T Tuple(UInt64, String), NT Tuple(A UInt64, B String), NST Tuple(X UInt8, Y Tuple(P UInt8, Q String))') ORDER BY ID;
SELECT * FROM file(currentDatabase() || '_05815.csv', 'CSVWithNames', 'ID UInt64, NAME String, T Tuple(UInt64, String), NT Tuple(A UInt64, B String), NST Tuple(X UInt8, Y Tuple(P UInt8, Q String))') ORDER BY ID SETTINGS input_format_column_name_matching_mode = 'ignore_case';
SELECT * FROM format(CSVWithNames, 'id UInt64, t Tuple(UInt64, String), `t.1` UInt64', $$"id","t.1","t.2"
1,5,"x"$$);
SELECT * FROM format(CSVWithNames, 'id UInt64, t Tuple(UInt64, String)', $$"id","t","t.1","t.2"
1,5,6,"x"$$); -- { serverError INCORRECT_DATA }
SELECT * FROM format(CSVWithNames, 'id UInt64, t Tuple(`a.b` UInt8, a Tuple(b UInt8))', $$"id","t.a.b","t.a.b"
1,1,2$$);
SELECT * FROM format(CSV, 'Name String, name String, t Tuple(UInt8)', $$NAME,foo,1$$);
SELECT * FROM format(CSVWithNames, '`t.a` Tuple(b UInt8), t Tuple(a Tuple(b UInt8, c UInt8))', $$"t.a.b","t.a.c"
1,2$$); -- { serverError INCORRECT_DATA }
SELECT * FROM format(CSVWithNames, 'Ta Tuple(a UInt8), tA Tuple(a UInt8)', $$"Ta.a","tA.a"
1,2$$);
SELECT * FROM format(CSVWithNames, 'Ta Tuple(a UInt8), tA Tuple(a UInt8)', $$"ta.a"
1$$); -- { serverError INCORRECT_DATA }
SELECT * FROM format(CSVWithNames, '`t.a` Tuple(b UInt8), t Tuple(a Tuple(b UInt8, c UInt8))', $$"t.a.b","t.a.b","t.a.c"
1,2,3$$) SETTINGS input_format_with_names_use_header = 0;
SELECT * FROM format(CSV, 'Ta Tuple(a String), tA Tuple(a String)', $$ta.a,x$$);
SELECT * FROM format(CSV, '`t.a` Tuple(b String), t Tuple(a Tuple(b String))', $$t.a.b,x$$);
SELECT * FROM format(CSV, '`T.a` String, `t.A` String, t Tuple(a String)', $$t.a,x,y$$);

DROP TABLE tr;
