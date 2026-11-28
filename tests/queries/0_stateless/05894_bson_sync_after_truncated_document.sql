-- A truncated document makes the reader fail before the size of the document is read. `syncAfterError` then has nothing
-- of the document to skip, and must not compute a negative remainder (UBSan: unsigned integer overflow).

SET input_format_parallel_parsing = 0;
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('01')) SETTINGS input_format_allow_errors_num = 1;
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('0100')) SETTINGS input_format_allow_errors_num = 1;

-- The size field is truncated, and its partial value is larger than the number of bytes read.
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('05')) SETTINGS input_format_allow_errors_num = 1;
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('0500')) SETTINGS input_format_allow_errors_num = 1;
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('050000')) SETTINGS input_format_allow_errors_num = 1;

-- The same after a valid document `{x: 1}`.
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('0C000000107800010000000005')) SETTINGS input_format_allow_errors_num = 1;
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('0C000000107800010000000001')) SETTINGS input_format_allow_errors_num = 1;

-- The size is read, but the body of the document is truncated.
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('0C00000010780001')) SETTINGS input_format_allow_errors_num = 1;
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('0C0000001078000100000000' || '0C00000010780002')) SETTINGS input_format_allow_errors_num = 1;

-- The body of the document is truncated inside a field name, or inside a field that is skipped.
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('0C00000010')) SETTINGS input_format_allow_errors_num = 1;
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('0C0000001078')) SETTINGS input_format_allow_errors_num = 1;
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('0C000000107900' || '01')) SETTINGS input_format_allow_errors_num = 1, input_format_skip_unknown_fields = 1;
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('0C0000000B7900' || '61')) SETTINGS input_format_allow_errors_num = 1, input_format_skip_unknown_fields = 1;
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('0C00000005790005000000' || '00')) SETTINGS input_format_allow_errors_num = 1, input_format_skip_unknown_fields = 1;
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('140000001078000100000010780002')) SETTINGS input_format_allow_errors_num = 1, input_format_skip_duplicated_fields = 1;
-- A nested field name is truncated.
SELECT * FROM format(BSONEachRow, 'x Tuple(a UInt8)', unhex('140000000378000C0000001061')) SETTINGS input_format_allow_errors_num = 1;
SELECT * FROM format(BSONEachRow, 'x Map(String, UInt8)', unhex('140000000378000C0000001061')) SETTINGS input_format_allow_errors_num = 1;
SELECT * FROM format(BSONEachRow, 'x Array(UInt8)', unhex('140000000478000C0000001030')) SETTINGS input_format_allow_errors_num = 1;
-- The same after a valid document `{x: 1}`.
SELECT * FROM format(BSONEachRow, 'x UInt8', unhex('0C0000001078000100000000' || '0C0000001078')) SETTINGS input_format_allow_errors_num = 1;

SELECT 'ok';
