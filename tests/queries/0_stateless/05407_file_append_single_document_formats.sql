-- Tags: no-fasttest
-- no-fasttest: the Protobuf formats are not built in the Fast test.

-- A second INSERT into a non-empty file is rejected for formats whose output is a single document.

DROP TABLE IF EXISTS t_json_compact;
CREATE TABLE t_json_compact (id UInt64, s String) ENGINE = File(JSONCompact);
INSERT INTO t_json_compact VALUES (1, 'a'), (2, 'a');
INSERT INTO t_json_compact VALUES (11, 'b'); -- { serverError CANNOT_APPEND_TO_FILE }
SELECT 'JSONCompact', * FROM t_json_compact ORDER BY id;
DROP TABLE t_json_compact;

DROP TABLE IF EXISTS t_json_compact_strings;
CREATE TABLE t_json_compact_strings (id UInt64, s String) ENGINE = File(JSONCompactStrings);
INSERT INTO t_json_compact_strings VALUES (1, 'a');
INSERT INTO t_json_compact_strings VALUES (11, 'b'); -- { serverError CANNOT_APPEND_TO_FILE }
DROP TABLE t_json_compact_strings;

DROP TABLE IF EXISTS t_protobuf_list;
CREATE TABLE t_protobuf_list (id UInt64, s String) ENGINE = File(ProtobufList)
SETTINGS format_schema_source = 'string', format_schema = 'syntax = "proto3"; message M05407 { uint64 id = 1; string s = 2; }', format_schema_message_name = 'M05407';
INSERT INTO t_protobuf_list VALUES (1, 'a'), (2, 'a');
INSERT INTO t_protobuf_list VALUES (11, 'b'); -- { serverError CANNOT_APPEND_TO_FILE }
SELECT 'ProtobufList', * FROM t_protobuf_list ORDER BY id;
DROP TABLE t_protobuf_list;

DROP TABLE IF EXISTS t_protobuf_single;
CREATE TABLE t_protobuf_single (id UInt64, s String) ENGINE = File(ProtobufSingle)
SETTINGS format_schema_source = 'string', format_schema = 'syntax = "proto3"; message M05407 { uint64 id = 1; string s = 2; }', format_schema_message_name = 'M05407';
INSERT INTO t_protobuf_single VALUES (1, 'a');
INSERT INTO t_protobuf_single VALUES (11, 'b'); -- { serverError CANNOT_APPEND_TO_FILE }
SELECT 'ProtobufSingle', * FROM t_protobuf_single;
DROP TABLE t_protobuf_single;

DROP TABLE IF EXISTS t_protobuf;
CREATE TABLE t_protobuf (id UInt64, s String) ENGINE = File(Protobuf)
SETTINGS format_schema_source = 'string', format_schema = 'syntax = "proto3"; message M05407 { uint64 id = 1; string s = 2; }', format_schema_message_name = 'M05407';
INSERT INTO t_protobuf VALUES (1, 'a');
INSERT INTO t_protobuf VALUES (11, 'b');
SELECT 'Protobuf', * FROM t_protobuf ORDER BY id;
DROP TABLE t_protobuf;

DROP TABLE IF EXISTS t_json_each_row;
CREATE TABLE t_json_each_row (id UInt64, s String) ENGINE = File(JSONEachRow);
INSERT INTO t_json_each_row VALUES (1, 'a');
INSERT INTO t_json_each_row VALUES (11, 'b');
SELECT 'JSONEachRow', * FROM t_json_each_row ORDER BY id;
DROP TABLE t_json_each_row;

DROP TABLE IF EXISTS t_json_each_row_array;
CREATE TABLE t_json_each_row_array (id UInt64, s String) ENGINE = File(JSONEachRow) SETTINGS output_format_json_array_of_rows = 1;
INSERT INTO t_json_each_row_array VALUES (1, 'a');
INSERT INTO t_json_each_row_array VALUES (11, 'b'); -- { serverError CANNOT_APPEND_TO_FILE }
SELECT 'JSONEachRow array_of_rows', * FROM t_json_each_row_array ORDER BY id;
DROP TABLE t_json_each_row_array;

DROP TABLE IF EXISTS t_template;
CREATE TABLE t_template (id UInt64, s String) ENGINE = File(Template)
SETTINGS format_template_row_format = '${id:CSV},${s:CSV}', format_template_rows_between_delimiter = '\n';
INSERT INTO t_template VALUES (1, 'a');
INSERT INTO t_template VALUES (11, 'b'); -- { serverError CANNOT_APPEND_TO_FILE }
DROP TABLE t_template;

DROP TABLE IF EXISTS t_template_resultset;
CREATE TABLE t_template_resultset (id UInt64, s String) ENGINE = File(Template)
SETTINGS format_template_row_format = '${id:CSV},${s:CSV}\n', format_template_rows_between_delimiter = '', format_template_resultset_format = '[${data}]';
INSERT INTO t_template_resultset VALUES (1, 'a');
INSERT INTO t_template_resultset VALUES (11, 'b'); -- { serverError CANNOT_APPEND_TO_FILE }
DROP TABLE t_template_resultset;

DROP TABLE IF EXISTS t_template_rows;
CREATE TABLE t_template_rows (id UInt64, s String) ENGINE = File(Template)
SETTINGS format_template_row_format = '${id:CSV},${s:CSV}\n', format_template_rows_between_delimiter = '', format_template_resultset_format = '${data}${rows:CSV}';
INSERT INTO t_template_rows VALUES (1, 'a');
INSERT INTO t_template_rows VALUES (11, 'b'); -- { serverError CANNOT_APPEND_TO_FILE }
DROP TABLE t_template_rows;

DROP TABLE IF EXISTS t_template_appendable;
CREATE TABLE t_template_appendable (id UInt64, s String) ENGINE = File(Template)
SETTINGS format_template_row_format = '${id:CSV},${s:CSV}\n', format_template_rows_between_delimiter = '';
INSERT INTO t_template_appendable VALUES (1, 'a');
INSERT INTO t_template_appendable VALUES (11, 'b');
DROP TABLE t_template_appendable;

INSERT INTO FUNCTION file(currentDatabase() || '_05407_template.csv', Template, 'id UInt64, s String') SELECT 1, 'a'
SETTINGS format_template_row_format = '${id:CSV},${s:CSV}\n', format_template_rows_between_delimiter = '', format_template_resultset_format = '${data}', engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05407_template.csv', Template, 'id UInt64, s String') SELECT 11, 'b'
SETTINGS format_template_row_format = '${id:CSV},${s:CSV}\n', format_template_rows_between_delimiter = '', format_template_resultset_format = '${data}';
SELECT 'Template resultset', * FROM file(currentDatabase() || '_05407_template.csv', CSV, 'id UInt64, s String') ORDER BY id;

DROP TABLE IF EXISTS t_custom_separated;
CREATE TABLE t_custom_separated (id UInt64, s String) ENGINE = File(CustomSeparated)
SETTINGS format_custom_row_between_delimiter = ';';
INSERT INTO t_custom_separated VALUES (1, 'a'), (2, 'a');
INSERT INTO t_custom_separated VALUES (11, 'b'); -- { serverError CANNOT_APPEND_TO_FILE }
SELECT 'CustomSeparated', * FROM t_custom_separated ORDER BY id;
DROP TABLE t_custom_separated;
