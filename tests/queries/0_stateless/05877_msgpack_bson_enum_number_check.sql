-- Tags: no-fasttest
-- no-fasttest: the MsgPack format is not built in the fast test

-- A number read into an Enum must be one of its elements (check_conversion_from_numbers_to_enum, on by default), as for CAST.
-- Rejected reads cast to the underlying integer, so a stored non-element would be printed rather than fail the output.
SELECT 'MsgPack';
SELECT * FROM format(MsgPack, 'e Enum8(''a'' = 5, ''b'' = 6, ''m'' = -3)', x'0506fd');
SELECT * FROM format(MsgPack, 'e Enum8(''m'' = -3)', x'd0fd');
SELECT CAST(e, 'Int8') FROM format(MsgPack, 'e Enum8(''a'' = 5, ''b'' = 6)', x'07'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
-- uint16 261, as MsgPack writes 261::Int16
SELECT CAST(e, 'Int8') FROM format(MsgPack, 'e Enum8(''a'' = 5, ''b'' = 6)', x'cd0105'); -- { serverError CANNOT_CONVERT_TYPE }
-- uint64 18446744073709551615
SELECT CAST(e, 'Int8') FROM format(MsgPack, 'e Enum8(''x'' = -1)', x'cfffffffffffffffff'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT * FROM format(MsgPack, 'e Enum16(''c'' = 600)', x'cd0258');
SELECT CAST(e, 'Int16') FROM format(MsgPack, 'e Enum16(''c'' = 600)', x'cd0259'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(a, 'Array(Int8)') FROM format(MsgPack, 'a Array(Enum8(''a'' = 5, ''b'' = 6))', x'920507'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(n, 'Nullable(Int8)') FROM format(MsgPack, 'n Nullable(Enum8(''a'' = 5, ''b'' = 6))', x'07'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(m, 'Map(Int8, UInt8)') FROM format(MsgPack, 'm Map(Enum8(''a'' = 5, ''b'' = 6), UInt8)', x'810701'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(t, 'Tuple(Int8)') FROM format(MsgPack, 't Tuple(Enum8(''a'' = 5, ''b'' = 6))', x'9107'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(m, 'Map(UInt8, Int8)') FROM format(MsgPack, 'm Map(UInt8, Enum8(''a'' = 5, ''b'' = 6))', x'810107'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(e, 'Int8') FROM format(MsgPack, 'e Enum8(''a'' = 5, ''b'' = 6)', x'07') SETTINGS check_conversion_from_numbers_to_enum = 0;
SELECT CAST(e, 'Int8') FROM format(MsgPack, 'e Enum8(''a'' = 5, ''b'' = 6)', x'cd0105') SETTINGS check_conversion_from_numbers_to_enum = 0;

SELECT 'BSONEachRow';
-- int32 -3, then int64 5
SELECT * FROM format(BSONEachRow, 'e Enum8(''a'' = 5, ''m'' = -3)', x'0C000000106500FDFFFFFF0010000000126500050000000000000000');
SELECT CAST(e, 'Int8') FROM format(BSONEachRow, 'e Enum8(''a'' = 5, ''b'' = 6)', x'0C0000001065000700000000'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(e, 'Int8') FROM format(BSONEachRow, 'e Enum8(''a'' = 5, ''b'' = 6)', x'0C0000001065000501000000'); -- { serverError CANNOT_CONVERT_TYPE }
-- int64 -3
SELECT * FROM format(BSONEachRow, 'e Enum8(''m'' = -3)', x'10000000126500FDFFFFFFFFFFFFFF00');
-- int64 70000
SELECT CAST(e, 'Int16') FROM format(BSONEachRow, 'e Enum16(''c'' = 600)', x'10000000126500701101000000000000'); -- { serverError CANNOT_CONVERT_TYPE }
-- bool true
SELECT * FROM format(BSONEachRow, 'e Enum8(''t'' = 1)', x'090000000865000100');
-- bool byte 0xff
SELECT CAST(e, 'Int16') FROM format(BSONEachRow, 'e Enum16(''m'' = -1)', x'09000000086500FF00'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT * FROM format(BSONEachRow, 'e Enum16(''c'' = 600)', x'0C0000001065005802000000');
SELECT CAST(e, 'Int16') FROM format(BSONEachRow, 'e Enum16(''c'' = 600)', x'0C0000001065005902000000'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(a, 'Array(Int8)') FROM format(BSONEachRow, 'a Array(Enum8(''a'' = 5, ''b'' = 6))', x'1B0000000461001300000010300005000000103100070000000000'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(e, 'Nullable(Int8)') FROM format(BSONEachRow, 'e Nullable(Enum8(''a'' = 5, ''b'' = 6))', x'0C0000001065000700000000'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(t, 'Tuple(Int8)') FROM format(BSONEachRow, 't Tuple(Enum8(''a'' = 5, ''b'' = 6))', x'140000000474000C000000103100070000000000'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
SELECT CAST(m, 'Map(String, Int8)') FROM format(BSONEachRow, 'm Map(String, Enum8(''a'' = 5, ''b'' = 6))', x'14000000036D000C000000106B00070000000000'); -- { serverError UNKNOWN_ELEMENT_OF_ENUM }
-- int32 5, then int32 7: the row with 7 is skipped
SELECT CAST(e, 'Int8') FROM format(BSONEachRow, 'e Enum8(''a'' = 5, ''b'' = 6)', x'0C00000010650005000000000C0000001065000700000000') SETTINGS input_format_allow_errors_num = 1;
SELECT CAST(e, 'Int8') FROM format(BSONEachRow, 'e Enum8(''a'' = 5, ''b'' = 6)', x'0C0000001065000700000000') SETTINGS check_conversion_from_numbers_to_enum = 0;
SELECT CAST(e, 'Int8') FROM format(BSONEachRow, 'e Enum8(''a'' = 5, ''b'' = 6)', x'0C0000001065000501000000') SETTINGS check_conversion_from_numbers_to_enum = 0;
