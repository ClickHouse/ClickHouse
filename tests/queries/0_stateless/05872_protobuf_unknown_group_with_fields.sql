-- Tags: no-fasttest
-- An unknown field encoded as a non-empty proto2 group is skipped.
SET format_schema_source = 'string';
SET format_schema = 'syntax = "proto3"; message M { int32 x = 1; string str = 2; }';
SET format_schema_message_name = 'M';

SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x4c');
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x08\x96\x01\x4c');
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x09\x01\x02\x03\x04\x05\x06\x07\x08\x4c');
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x0a\x03abc\x4c');
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x0d\x01\x02\x03\x04\x4c');
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x0b\x08\x01\x0c\x4c');
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x4b\x08\x01\x0a\x01z\x4c\x12\x02ok');
SELECT * FROM format(Protobuf, 'x Int32, str String', '\x0a\x08\x07\x12\x02ok\x4b\x08\x01\x4c\x06\x08\x08\x12\x02no');

SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x08\x01\x4e'); -- { serverError UNKNOWN_PROTOBUF_FORMAT }
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x08\x01'); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }

SET format_schema = 'syntax = "proto2"; message M { optional int32 x = 1; optional group G = 2 { optional int32 a = 1; } }';
SELECT * FROM format(ProtobufSingle, 'x Int32, g Tuple(a Int32)', '\x08\x07\x13\x08\x05\x1b\x08\x01\x1c\x14');
