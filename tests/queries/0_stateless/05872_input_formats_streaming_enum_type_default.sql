-- Tags: no-fasttest
-- Tag no-fasttest: the Avro and Protobuf formats are not in the fasttest build.

-- A value the reader has no source for (a column or element missing from the file or the message, or the row of a
-- record that failed to parse) gets the default of the column type. For an Enum without 0 that is its smallest
-- element, not the invalid value 0, also inside a Tuple or a JSON typed path. A Nullable column stays NULL.

SELECT 'Avro, a column missing from the schema';
INSERT INTO FUNCTION file(currentDatabase() || '_05872.avro', Avro) SELECT toInt8(1) AS x SETTINGS engine_file_truncate_on_insert = 1;
SELECT * FROM file(currentDatabase() || '_05872.avro', Avro, 'x Int8, e Enum8(\'a\' = 1, \'b\' = 2), t Tuple(s String, e Enum16(\'x\' = 5, \'y\' = 7)), ne Nullable(Enum8(\'a\' = 1, \'b\' = 2)), j JSON(e Enum8(\'a\' = 1, \'b\' = 2)), z Enum8(\'neg\' = -1, \'zero\' = 0)')
SETTINGS input_format_avro_allow_missing_fields = 1;
SELECT count() FROM file(currentDatabase() || '_05872.avro', Avro, 'x Int8, e Enum8(\'a\' = 1, \'b\' = 2)') WHERE e = 'a'
SETTINGS input_format_avro_allow_missing_fields = 1;

SET format_schema_source = 'string';
SET format_schema = 'syntax = "proto3"; message M { int32 x = 1; N n = 2; repeated N r = 3; } message N { int32 y = 1; }';
SET format_schema_message_name = 'M';

-- x = 7, n = {y = 5}, r = [{y = 1}, {y = 2}]. `e`, `t` and `ne` are missing from M, `n.e` from the nested message,
-- and `r.e` is the missing sibling of `r.y`, so it gets an array of the same length.
SELECT 'Protobuf, a column missing from the message';
SELECT * FROM format(ProtobufSingle, 'x Int32, e Enum8(\'a\' = 1, \'b\' = 2), t Tuple(s String, e Enum16(\'x\' = 5, \'y\' = 7)), ne Nullable(Enum8(\'a\' = 1, \'b\' = 2)), n Tuple(y Int32, e Enum8(\'a\' = 1, \'b\' = 2)), `r.y` Array(Int32), `r.e` Array(Enum8(\'a\' = 1, \'b\' = 2)), z Enum8(\'neg\' = -1, \'zero\' = 0)', '\x08\x07\x12\x02\x08\x05\x1a\x02\x08\x01\x1a\x02\x08\x02');
SELECT * FROM format(ProtobufSingle, 'x Int32, r Nested(y Int32, e Enum8(\'a\' = 1, \'b\' = 2))', '\x08\x07\x1a\x02\x08\x01\x1a\x02\x08\x02')
SETTINGS flatten_nested = 0;

SELECT 'ProtobufList, a column missing from the message';
SET format_schema = 'syntax = "proto3"; message Envelope { message M { int32 x = 1; } repeated M row = 1; }';
SELECT * FROM format(ProtobufList, 'x Int32, e Enum8(\'a\' = 1, \'b\' = 2), t Tuple(s String, e Enum16(\'x\' = 5, \'y\' = 7)), ne Nullable(Enum8(\'a\' = 1, \'b\' = 2))', '\x04\x0a\x02\x08\x07');

-- A wrapper message without `value` gives 0, or the default of the column type for an Enum without 0, also for a
-- StringValue read into a non-string type. An absent wrapper is NULL.
SELECT 'Protobuf, a google wrapper without value';
SET input_format_protobuf_flatten_google_wrappers = 1;
SET format_schema = 'syntax = "proto3"; import "google/protobuf/wrappers.proto"; message W { google.protobuf.Int32Value w = 1; int32 k = 2; google.protobuf.StringValue s = 3; }';
SET format_schema_message_name = 'W';
SELECT * FROM format(ProtobufSingle, 'w Nullable(Enum8(\'a\' = 1, \'b\' = 2)), k Int32', '\x0a\x00\x10\x01');
SELECT * FROM format(ProtobufSingle, 'w Nullable(Enum8(\'neg\' = -1, \'zero\' = 0)), k Int32', '\x0a\x00\x10\x01');
SELECT * FROM format(ProtobufSingle, 'w Nullable(Enum16(\'neg\' = -5, \'zero\' = 0)), k Int32', '\x0a\x00\x10\x01');
SELECT * FROM format(ProtobufSingle, 'w Nullable(Tuple(Enum16(\'neg\' = -5, \'zero\' = 0))), k Int32', '\x0a\x00\x10\x01');
SELECT * FROM format(ProtobufSingle, 'w Nullable(Tuple(Enum8(\'a\' = 1, \'b\' = 2))), k Int32', '\x0a\x00\x10\x01');
SELECT * FROM format(ProtobufSingle, 'w Nullable(Int32), k Int32', '\x0a\x00\x10\x01');
SELECT * FROM format(ProtobufSingle, 'w Nullable(Enum8(\'a\' = 1, \'b\' = 2)), k Int32', '\x10\x01');
SELECT * FROM format(ProtobufSingle, 's Nullable(Date), k Int32', '\x1a\x00\x10\x01');
SELECT * FROM format(ProtobufSingle, 's Nullable(UUID), k Int32', '\x1a\x00\x10\x01');

-- The value 0 is written as a wrapper without `value`, so it must read back as the element 0.
SET output_format_protobuf_nullables_with_google_wrappers = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05872.pb', Protobuf, 'w Nullable(Enum8(\'neg\' = -1, \'zero\' = 0, \'pos\' = 1)), k Int32')
SELECT * FROM values('w Nullable(Enum8(\'neg\' = -1, \'zero\' = 0, \'pos\' = 1)), k Int32', ('neg', 1), ('zero', 2), ('pos', 3), (NULL, 4))
SETTINGS engine_file_truncate_on_insert = 1;
SELECT * FROM file(currentDatabase() || '_05872.pb', Protobuf, 'w Nullable(Enum8(\'neg\' = -1, \'zero\' = 0, \'pos\' = 1)), k Int32') ORDER BY k;

SELECT 'FileLog, handle_error_mode = stream';
DROP TABLE IF EXISTS file_log;
DROP TABLE IF EXISTS dst;
INSERT INTO FUNCTION file({CLICKHOUSE_DATABASE:String} || '_05872_filelog/a.jsonl', LineAsString)
SELECT arrayJoin(['{"k": 1, "e": "b", "t": {"s": "p", "e": "y"}, "ne": "b"}', 'not json'])
SETTINGS engine_file_truncate_on_insert = 1;
CREATE TABLE file_log (k UInt8, e Enum8('a' = 1, 'b' = 2), t Tuple(s String, e Enum16('x' = 5, 'y' = 7)), ne Nullable(Enum8('a' = 1, 'b' = 2)))
ENGINE = FileLog({CLICKHOUSE_DATABASE:String} || '_05872_filelog/', 'JSONEachRow') SETTINGS handle_error_mode = 'stream';
CREATE TABLE dst ENGINE = MergeTree ORDER BY k AS SELECT k, e, t, ne, isNotNull(_error) AS failed FROM file_log
SETTINGS stream_like_engine_allow_direct_select = 1;
SELECT * FROM dst ORDER BY k;
SELECT count() FROM dst WHERE e = 'a';
DROP TABLE file_log;
DROP TABLE dst;
