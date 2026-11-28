-- Tags: no-fasttest
-- no-fasttest: the ORC format is not available in fasttest builds

-- A top-level ORC column named `a.x` is read, not the field `x` of a struct `a`, in both file orders.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_struct_first.orc', ORC) SELECT CAST(tuple(11), 'Tuple(x Int32)') AS a, toInt32(22) AS `a.x`;
INSERT INTO FUNCTION file(currentDatabase() || '_column_first.orc', ORC) SELECT toInt32(22) AS `a.x`, CAST(tuple(11), 'Tuple(x Int32)') AS a;
INSERT INTO FUNCTION file(currentDatabase() || '_no_x.orc', ORC) SELECT CAST(tuple(11), 'Tuple(y Int32)') AS a, toInt32(22) AS `a.x`;
INSERT INTO FUNCTION file(currentDatabase() || '_folded.orc', ORC) SELECT CAST(tuple(11), 'Tuple(x Int32)') AS a, toInt32(22) AS `A.X`;

SELECT * FROM file(currentDatabase() || '_struct_first.orc', ORC, '`a.x` Int32');
SELECT * FROM file(currentDatabase() || '_struct_first.orc', ORC, 'a Tuple(x Int32), `a.x` Int32');
SELECT * FROM file(currentDatabase() || '_struct_first.orc', ORC, '`a.x` Int32') WHERE `a.x` = 22 SETTINGS input_format_orc_filter_push_down = 1;
SELECT count() FROM file(currentDatabase() || '_struct_first.orc', ORC, '`a.x` Int32') WHERE indexHint(`a.x` = 23) SETTINGS input_format_orc_filter_push_down = 1;
SELECT * FROM file(currentDatabase() || '_column_first.orc', ORC, '`a.x` Int32');
SELECT * FROM file(currentDatabase() || '_column_first.orc', ORC, 'a Tuple(x Int32), `a.x` Int32');
SELECT * FROM file(currentDatabase() || '_no_x.orc', ORC, '`a.x` Int32');
SELECT * FROM file(currentDatabase() || '_folded.orc', ORC, '`a.x` Int32') SETTINGS input_format_orc_case_insensitive_column_matching = 1;
