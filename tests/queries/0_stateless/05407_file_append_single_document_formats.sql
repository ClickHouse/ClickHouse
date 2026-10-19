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
