-- Tags: no-fasttest
-- Tag no-fasttest: the Protobuf input format needs the protobuf library, which the fasttest build omits.

-- A field that the schema does not contain is skipped by the reader. When such a field uses the
-- deprecated proto2 "group" encoding (tag wire type 3 opens the group, wire type 4 closes it),
-- ProtobufReader::ignoreGroup consumes it and returns at the matching end marker.
-- Format: docs/reference/formats/Protobuf/ProtobufSingle.mdx (one message, no length delimiter).
-- Behaviour as implemented today: an empty unknown group is consumed and the known fields of the
-- message are decoded, while a group that carries a field of any wire type is reported as
-- UNKNOWN_PROTOBUF_FORMAT even though the group itself is closed. Both halves are asserted here.

SET format_schema_source = 'string';
SET format_schema = 'syntax = "proto3"; message M { int32 x = 1; string str = 2; }';
SET format_schema_message_name = 'M';

-- Every payload below starts with the two fields of the schema, x = 7 and str = 'ok'
-- (\x08\x07\x12\x02ok), and is followed by field 9, which the schema does not have:
-- \x4b = (9 << 3) | 3 opens the group, \x4c = (9 << 3) | 4 closes it.

SELECT 'empty group';
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x4c');

-- \x08 in the group: field 1, wire type 0 (varint), value 1.
SELECT 'varint in group';
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x08\x01\x4c'); -- { serverError UNKNOWN_PROTOBUF_FORMAT }

-- \x09: field 1, wire type 1 (64 bit), eight bytes of payload.
SELECT 'bits64 in group';
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x09\x00\x00\x00\x00\x00\x00\x00\x00\x4c'); -- { serverError UNKNOWN_PROTOBUF_FORMAT }

-- \x0a: field 1, wire type 2 (length delimited), length 3, payload 'abc'.
SELECT 'length delimited in group';
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x0a\x03abc\x4c'); -- { serverError UNKNOWN_PROTOBUF_FORMAT }

-- \x0b: field 1, wire type 3, a group nested in the unknown group and closed right away.
SELECT 'nested group';
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x0b\x4c\x4c'); -- { serverError UNKNOWN_PROTOBUF_FORMAT }

-- \x0d: field 1, wire type 5 (32 bit), four bytes of payload.
SELECT 'bits32 in group';
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x0d\x00\x00\x00\x00\x4c'); -- { serverError UNKNOWN_PROTOBUF_FORMAT }
