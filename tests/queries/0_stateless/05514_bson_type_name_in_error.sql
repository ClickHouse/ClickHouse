-- `getBSONTypeName` (src/Formats/BSONTypes.cpp) names a BSON type in the error that
-- BSONEachRow input reports when a field cannot go into the target column. Its switch
-- has no `default:` and no return after the switch, so a type accepted by `getBSONType`
-- but missing a case would fall off the end of the function. Every type byte that
-- `getBSONType` accepts (0x01..0x13, 0x7F, 0xFF) is therefore checked below; the type
-- names are the ones from https://bsonspec.org/spec.html.
--
-- `readAndInsertUUID` rejects a field on its type byte alone, before any value byte is
-- read, so one minimal 8-byte document is enough for every type:
--   08 00 00 00   document size = 8
--   <type>        the type byte under test
--   75 00         key "u\0"
--   00            document terminator
-- A well-formed ILLEGAL_COLUMN for each of them means the name was produced and the
-- parser stayed on its feet.

-- 0x06 Undefined
SELECT * FROM format(BSONEachRow, 'u UUID', x'0800000006750000'); -- { serverError ILLEGAL_COLUMN }
-- 0x07 ObjectId
SELECT * FROM format(BSONEachRow, 'u UUID', x'0800000007750000'); -- { serverError ILLEGAL_COLUMN }
-- 0x0B Regexp
SELECT * FROM format(BSONEachRow, 'u UUID', x'080000000b750000'); -- { serverError ILLEGAL_COLUMN }
-- 0x0C DBPointer
SELECT * FROM format(BSONEachRow, 'u UUID', x'080000000c750000'); -- { serverError ILLEGAL_COLUMN }
-- 0x0D JavaScript code
SELECT * FROM format(BSONEachRow, 'u UUID', x'080000000d750000'); -- { serverError ILLEGAL_COLUMN }
-- 0x0E Symbol
SELECT * FROM format(BSONEachRow, 'u UUID', x'080000000e750000'); -- { serverError ILLEGAL_COLUMN }
-- 0x0F JavaScript code w/ scope
SELECT * FROM format(BSONEachRow, 'u UUID', x'080000000f750000'); -- { serverError ILLEGAL_COLUMN }
-- 0x11 Timestamp
SELECT * FROM format(BSONEachRow, 'u UUID', x'0800000011750000'); -- { serverError ILLEGAL_COLUMN }
-- 0x13 Decimal128
SELECT * FROM format(BSONEachRow, 'u UUID', x'0800000013750000'); -- { serverError ILLEGAL_COLUMN }
-- 0x7F Max key
SELECT * FROM format(BSONEachRow, 'u UUID', x'080000007f750000'); -- { serverError ILLEGAL_COLUMN }
-- 0xFF Min key
SELECT * FROM format(BSONEachRow, 'u UUID', x'08000000ff750000'); -- { serverError ILLEGAL_COLUMN }

-- The remaining type bytes 0x01..0x05, 0x08..0x0A, 0x10 and 0x12 are already named by
-- 04112_bson_nullable_array_read_rollback and skipped by
-- 05359_bson_skip_unknown_field_types. 0x0A Null is the one name no input can ask for:
-- `BSONEachRowRowInputFormat::readField` handles NULL_VALUE before the type switch and
-- schema inference maps it to no type at all.

-- The three type bytes above that a String column does accept decode to a value, which
-- fixes what those bytes mean: 0x0E and 0x0D carry a length-prefixed string, 0x07 carries
-- twelve raw bytes. Reading one byte too few or too many would miss the document
-- terminator, so the decoded value is what proves the encoding.

-- Symbol: size 15 | 0E | key "s\0" | length 3 | "ab\0" | terminator
SELECT count(), max(s) FROM format(BSONEachRow, 's String', x'0f0000000e73000300000061620000');
-- JavaScript code: size 15 | 0D | key "s\0" | length 3 | "cd\0" | terminator
SELECT count(), max(s) FROM format(BSONEachRow, 's String', x'0f0000000d73000300000063640000');
-- ObjectId: size 20 | 07 | key "s\0" | twelve bytes | terminator
SELECT count(), hex(max(s)) FROM format(BSONEachRow, 's String', x'14000000077300000102030405060708090a0b00');
