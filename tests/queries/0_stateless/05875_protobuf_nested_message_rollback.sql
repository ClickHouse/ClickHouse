-- Tags: no-fasttest
-- Tag no-fasttest: the Protobuf format is not in the fasttest build.

SET format_schema_source = 'string';
SET format_schema_message_name = 'M';

SET format_schema = 'syntax = "proto3"; message M { int32 x = 1; N n = 2; repeated N r = 3; } message N { int32 y = 1; }';

-- `e` has no field in N, so the reader fills it with the default.
SELECT * FROM format(ProtobufSingle, 'x Int32, n Tuple(y Int32, e Int32)', '\x08\x01\x12\x02\x08\x05');
SELECT * FROM format(ProtobufSingle, 'x Int32, r Array(Tuple(y Int32, e Int32))', '\x08\x01\x1a\x02\x08\x05');

-- The nested message has length 1: it holds the key of `y`, and the value of `y` lies past its end.
SELECT * FROM format(ProtobufSingle, 'x Int32, n Tuple(y Int32, e Int32)', '\x12\x01\x08\x05'); -- { serverError UNKNOWN_PROTOBUF_FORMAT }
SELECT * FROM format(ProtobufSingle, 'x Int32, r Array(Tuple(y Int32, e Int32))', '\x1a\x01\x08\x05'); -- { serverError UNKNOWN_PROTOBUF_FORMAT }
SELECT * FROM format(ProtobufList, 'x Int32, n Tuple(y Int32, e Int32)', '\x06\x0a\x04\x12\x01\x08\x05'); -- { serverError UNKNOWN_PROTOBUF_FORMAT }

-- The empty default of a string field cannot be read into Int32,
-- both when `n` is present without `s` and when `n` is absent.
SET format_schema = 'syntax = "proto3"; message M { int32 x = 1; N n = 2; } message N { int32 y = 1; string s = 2; }';
SELECT * FROM format(ProtobufSingle, 'x Int32, n Tuple(y Int32, s Int32, e Int32)', '\x08\x01\x12\x02\x08\x05'); -- { serverError PROTOBUF_BAD_CAST }
SELECT * FROM format(ProtobufSingle, 'x Int32, n Tuple(y Int32, s Int32, e Int32)', '\x08\x01'); -- { serverError PROTOBUF_BAD_CAST }
