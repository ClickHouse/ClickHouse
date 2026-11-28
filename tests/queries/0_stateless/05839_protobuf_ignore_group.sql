-- Tags: no-fasttest
-- Tag no-fasttest: the Protobuf input format needs the protobuf library, which the fasttest build omits.

-- A field that the schema does not contain is skipped by the reader. When such a field uses the
-- deprecated proto2 "group" encoding (tag wire type 3 opens the group, wire type 4 closes it),
-- ProtobufReader::ignoreGroup walks the group and returns at the matching end marker.
-- Format: docs/reference/formats/Protobuf/ProtobufSingle.mdx (one message, no length delimiter).

SET format_schema_source = 'string';
SET format_schema = 'syntax = "proto3"; message M { int32 x = 1; string str = 2; }';
SET format_schema_message_name = 'M';

-- Every payload below starts with the two fields of the schema, x = 7 and str = 'ok'
-- (\x08\x07\x12\x02ok), followed by field 9, which the schema does not have:
-- \x4b = (9 << 3) | 3 opens the group, \x4c = (9 << 3) | 4 closes it.

-- A closed, empty unknown group is skipped and the two known fields are decoded.
SELECT 'empty group';
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x4c');

-- \x4e = (9 << 3) | 6. Wire type 6 does not exist, so the payload is corrupt.
SELECT 'invalid wire type in group';
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x4e'); -- { serverError UNKNOWN_PROTOBUF_FORMAT }

-- The payloads below end inside the group, so the field that is being skipped runs out of input.
-- \x08, \x09, \x0a and \x0d are field 1 with wire type 0, 1, 2 and 5.

-- A varint whose continuation bit is set and which has no next byte.
SELECT 'truncated varint in group';
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x08\x80'); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }

-- A 64 bit field that carries two of its eight bytes.
SELECT 'truncated 64 bit field in group';
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x09\x00\x00'); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }

-- A length delimited field that announces 127 bytes and carries none.
SELECT 'truncated length delimited field in group';
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x0a\x7f'); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }

-- A 32 bit field that carries one of its four bytes.
SELECT 'truncated 32 bit field in group';
SELECT * FROM format(ProtobufSingle, 'x Int32, str String', '\x08\x07\x12\x02ok\x4b\x0d\x00'); -- { serverError ATTEMPT_TO_READ_AFTER_EOF }
