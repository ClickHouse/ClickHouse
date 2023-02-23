CREATE TABLE mytable
(
    operand Float64,
    min     Float64,
    max     Float64,
    count   UInt64,
    PRIMARY KEY (operand, min, max, count)
) ENGINE = MergeTree();

INSERT INTO mytable VALUES (3, -100, 200, 10), (0, 0, 10, 4), (3, 0, 10, 3), (4.333, 1, 11, 3), (4.34, 1, 11, 3), (-7.6, -10, 0, 4), (-6, -5, -1, 2), (1, 3, 0, 1), (3, 2, 5, 0);

SELECT operand, min, max, count, WIDTH_BUCKET(operand, min, max, count) FROM mytable WHERE count != 0;
SELECT '----------';
-- zero is not valid for count
SELECT operand, min, max, count, WIDTH_BUCKET(operand, min, max, count) FROM mytable WHERE count = 0; -- { serverError 36}
-- IntXX types
SELECT toInt64(operand) AS operand, toInt32(min) AS min, toInt16(max) AS max, count, WIDTH_BUCKET(operand, min, max, count) FROM mytable WHERE count != 0;
SELECT '----------';
-- UIntXX types
SELECT toUInt8(operand) AS operand, toUInt16(min) AS min, toUInt32(max) AS max, count, WIDTH_BUCKET(operand, min, max, count) FROM mytable WHERE count != 0;
SELECT '----------';
-- Wrong argument types
SELECT WIDTH_BUCKET(1, 2, 3, -1); -- { serverError 43 }
SELECT WIDTH_BUCKET(1, 2, 3, 1.3); -- { serverError 43 }
SELECT WIDTH_BUCKET('a', 1, 2, 3); -- { serverError 43 }
SELECT WIDTH_BUCKET(1, toUInt128(42), 2, 3); -- { serverError 43 }
SELECT WIDTH_BUCKET(1, 2, toInt128(42), 3); -- { serverError 43 }
SELECT WIDTH_BUCKET(1, 2, 3, toInt256(42)); -- { serverError 43 }
-- Return type checks
SELECT toTypeName(WIDTH_BUCKET(1, 2, 3, toUInt8(1)));
SELECT toTypeName(WIDTH_BUCKET(1, 2, 3, toUInt16(1)));
SELECT toTypeName(WIDTH_BUCKET(1, 2, 3, toUInt32(1)));
SELECT toTypeName(WIDTH_BUCKET(1, 2, 3, toUInt64(1)));
-- Test handling ColumnConst
SELECT WIDTH_BUCKET(1, min, max, count) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(operand, 2, max, count) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(3, 3, max, count) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(operand, min, 4, count) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(5, min, 5, count) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(operand, 6, 6, count) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(7, 7, 7, count) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(operand, min, max, 8) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(9, min, max, 9) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(operand, 10, max, 10) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(11, 11, max, 11) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(operand, min, 12, 12) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(13, min, 13, 13) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(operand, 14, 14, 14) FROM mytable WHERE count != 0;
SELECT WIDTH_BUCKET(15, 15, 15, 15) FROM mytable WHERE count != 0;