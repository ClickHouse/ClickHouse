-- JSONEachRow: a repeated field is an error by default, with the setting the first value is kept.
SELECT * FROM format(JSONEachRow, 'name String, url String', '{"url":"a","url":"b","name":"x"}'); -- { serverError INCORRECT_DATA }
SELECT * FROM format(JSONEachRow, 'name String, url String', '{"url":"a","url":"b","name":"x"}') SETTINGS input_format_skip_duplicated_fields = 1;
SELECT * FROM format(JSONEachRow, 'name String, url String', '{"name":"x","url":"a","url":"b"}') SETTINGS input_format_json_ignore_unnecessary_fields = 0; -- { serverError INCORRECT_DATA }
SELECT * FROM format(JSONEachRow, 'name String, url String', '{"name":"x","url":"a","url":"b"}') SETTINGS input_format_json_ignore_unnecessary_fields = 0, input_format_skip_duplicated_fields = 1;

-- A trailing duplicate with input_format_skip_unknown_fields = 0 is skipped, an unknown field is still an error.
SELECT * FROM format(JSONEachRow, 'name String, url String', '{"name":"x","url":"a","url":"b"}') SETTINGS input_format_skip_unknown_fields = 0, input_format_skip_duplicated_fields = 1;
SELECT * FROM format(JSONEachRow, 'name String, url String', '{"name":"x","url":"a","other":"b"}') SETTINGS input_format_skip_unknown_fields = 0, input_format_skip_duplicated_fields = 1; -- { serverError INCORRECT_DATA }

-- Nested fields.
SELECT * FROM format(JSONEachRow, '`n.a` String, `n.b` String', '{"n":{"a":"1","a":"2","b":"3"}}') SETTINGS input_format_import_nested_json = 1, input_format_skip_duplicated_fields = 1;
SELECT * FROM format(JSONEachRow, '`n.a` String, `n.b` String', '{"n":{"a":"1"},"n":{"a":"2","b":"3"}}') SETTINGS input_format_import_nested_json = 1, input_format_skip_duplicated_fields = 1;

-- Schema inference: the type comes from the first value, also for a duplicate in a later row.
DESC format(JSONEachRow, '{"url":"a","url":1,"name":"x"}'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DESC format(JSONEachRow, '{"url":"a","url":1,"name":"x"}') SETTINGS input_format_skip_duplicated_fields = 1;
SELECT * FROM format(JSONEachRow, '{"url":"a","url":1,"name":"x"}') SETTINGS input_format_skip_duplicated_fields = 1;
SELECT * FROM format(JSONEachRow, '{"a":1}\n{"b":1,"b":"x"}') SETTINGS input_format_skip_duplicated_fields = 1;

SELECT * FROM format(JSONObjectEachRow, '{"r1":{"a":"x","a":"y","b":"z"}}') SETTINGS input_format_skip_duplicated_fields = 1;
SELECT *, toTypeName(id) FROM format(JSONObjectEachRow, '{"row":{"id":"duplicate","b":"ok"}}') SETTINGS input_format_skip_duplicated_fields = 1, format_json_object_each_row_column_for_object_name = 'id';
SELECT * FROM format(JSONObjectEachRow, '{"user1":{"id":"user1"}}') SETTINGS input_format_skip_duplicated_fields = 1, format_json_object_each_row_column_for_object_name = 'id';
SELECT * FROM format(JSONObjectEachRow, '{"r1":{"id":[1],"b":"x"},"r2":{"id":1,"b":"y"}}') SETTINGS input_format_skip_duplicated_fields = 1, format_json_object_each_row_column_for_object_name = 'id';

-- TSKV, BSONEachRow, Form: with a structure and with schema inference.
SELECT * FROM format(TSKV, 'a String, b String', 'a=x\ta=y\tb=z\n'); -- { serverError INCORRECT_DATA }
SELECT * FROM format(TSKV, 'a String, b String', 'a=x\ta=y\tb=z\n') SETTINGS input_format_skip_duplicated_fields = 1;
SELECT * FROM format(TSKV, 'a=x\ta=y\tb=z\n') SETTINGS input_format_skip_duplicated_fields = 1;
SELECT * FROM format(BSONEachRow, 'a String', x'1700000002610002000000780002610002000000790000'); -- { serverError INCORRECT_DATA }
SELECT * FROM format(BSONEachRow, 'a String', x'1700000002610002000000780002610002000000790000') SETTINGS input_format_skip_duplicated_fields = 1;
SELECT * FROM format(BSONEachRow, x'1700000002610002000000780002610002000000790000') SETTINGS input_format_skip_duplicated_fields = 1;
SELECT * FROM format(Form, 'a String, b String', 'a=x&a=y&b=z'); -- { serverError INCORRECT_DATA }
SELECT * FROM format(Form, 'a String, b String', 'a=x&a=y&b=z') SETTINGS input_format_skip_duplicated_fields = 1;
SELECT * FROM format(Form, 'a=x&a=y&b=z') SETTINGS input_format_skip_duplicated_fields = 1;

-- Schema inference skips a repeated value without inferring it: one deeper than max_parser_depth, a BSON Regexp.
SELECT * FROM format(JSONEachRow, concat('{"a":1,"a":', repeat('[', 30), repeat(']', 30), '}')) SETTINGS input_format_skip_duplicated_fields = 1, max_parser_depth = 20;
SELECT * FROM format(TSKV, concat('a=1\ta=', repeat('[', 30), repeat(']', 30), '\n')) SETTINGS input_format_skip_duplicated_fields = 1, max_parser_depth = 20;
SELECT * FROM format(Form, concat('a=1&a=', repeat('[', 30), repeat(']', 30))) SETTINGS input_format_skip_duplicated_fields = 1, max_parser_depth = 20;
SELECT * FROM format(JSONObjectEachRow, concat('{"r":{"id":', repeat('[', 30), repeat(']', 30), ',"b":"x"}}')) SETTINGS input_format_skip_duplicated_fields = 1, format_json_object_each_row_column_for_object_name = 'id', max_parser_depth = 20;
SELECT * FROM format(BSONEachRow, x'13000000106100010000000b61007800690000') SETTINGS input_format_skip_duplicated_fields = 1;

CREATE TABLE t (name String, url String) ENGINE = Memory;
INSERT INTO t SETTINGS input_format_skip_duplicated_fields = 1 FORMAT JSONEachRow {"url":"a","url":"b","name":"x"};

SELECT * FROM t;
DROP TABLE t;
