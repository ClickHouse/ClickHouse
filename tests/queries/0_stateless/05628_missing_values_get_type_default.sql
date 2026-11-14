-- Values absent from Native and JSON-format input get the default of their type, not the zero of the column.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_missing_default.native', Native) SELECT number + 1 AS id FROM numbers(2);

SELECT id, status FROM file(currentDatabase() || '_missing_default.native', Native, 'id UInt32, status Enum8(\'a\' = 1, \'b\' = 2)') ORDER BY id;
SELECT id, status FROM file(currentDatabase() || '_missing_default.native', Native, 'id UInt32, status Enum8(\'m\' = -3, \'n\' = 0, \'o\' = 4)') ORDER BY id;

DROP TABLE IF EXISTS events;
CREATE TABLE events (id UInt32, status Enum8('a' = 1, 'b' = 2)) ENGINE = MergeTree ORDER BY id;
INSERT INTO events SELECT * FROM file(currentDatabase() || '_missing_default.native', Native);
SELECT * FROM events ORDER BY id;
DROP TABLE events;

SELECT * FROM format(JSONColumns, 'id UInt32, status Enum8(\'a\' = 1, \'b\' = 2)', '{"id": [1, 2]}');

SELECT t FROM format(JSONEachRow, 't Tuple(n UInt8, status Enum8(\'a\' = 1, \'b\' = 2))', '{"t": {"n": 5}}\n{"t": {}}');
SELECT a FROM format(JSONEachRow, 'a Array(Tuple(n UInt8, status Enum8(\'a\' = 1, \'b\' = 2)))', '{"a": [{"n": 5}]}');
SELECT t FROM format(JSONEachRow, 't Tuple(n UInt8, u Tuple(status Enum8(\'a\' = 1, \'b\' = 2)))', '{"t": {"n": 5}}');
SELECT t FROM format(JSONEachRow, 't Tuple(n UInt8, s String, a Array(UInt8), x Nullable(Enum8(\'a\' = 1, \'b\' = 2)))', '{"t": {"n": 5}}');
SELECT t.b FROM format(JSONEachRow, 't Tuple(a Nothing, b UInt8)', '{"t": {"b": 1}}');
