-- A BSON int32 is signed: read into a wider integer column it is sign-extended, as CAST does.
SELECT * FROM format(BSONEachRow, 'x Int64', formatRowNoNewline('BSONEachRow', -1::Int32 AS x));
SELECT * FROM format(BSONEachRow, 'x Int64', formatRowNoNewline('BSONEachRow', -2147483648::Int32 AS x));
SELECT * FROM format(BSONEachRow, 'x Nullable(Int64)', formatRowNoNewline('BSONEachRow', -1::Int32 AS x));
SELECT * FROM format(BSONEachRow, 'x UInt64', formatRowNoNewline('BSONEachRow', -1::Int32 AS x));
SELECT * FROM format(BSONEachRow, 'a Array(Int64), m Map(String, Int64), t Tuple(Int64)', formatRowNoNewline('BSONEachRow', [-1, 5]::Array(Int32) AS a, map('k', -7::Int32) AS m, tuple(-3::Int32) AS t));
-- int32 -1, then int64 5000000000: the field is inferred as Int64.
SELECT x, toTypeName(x) FROM format(BSONEachRow, x'0C000000107800FFFFFFFF001000000012780000F2052A0100000000');
-- Columns of 32 bits or less are unchanged.
SELECT * FROM format(BSONEachRow, 'x Int32', formatRowNoNewline('BSONEachRow', -1::Int32 AS x));
SELECT * FROM format(BSONEachRow, 'x UInt32', formatRowNoNewline('BSONEachRow', -1::Int32 AS x));
SELECT * FROM format(BSONEachRow, 'x Int8', formatRowNoNewline('BSONEachRow', -1::Int32 AS x));
SELECT * FROM format(BSONEachRow, 'x UInt8', formatRowNoNewline('BSONEachRow', -1::Int32 AS x));
