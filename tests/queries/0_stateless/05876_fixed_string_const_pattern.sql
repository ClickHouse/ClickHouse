WITH
    toFixedString(substring(repeat(concat(toString(number), unhex('00F17E')), 16), 1, 1), 1) AS value,
    toFixedString(substring(repeat(unhex('A501F0803C12D769'), 8), 1, 1), 1) AS pattern,
    arrayStringConcat(arrayMap(i -> char(bitAnd(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 1+1))) AS expected_and,
    arrayStringConcat(arrayMap(i -> char(bitOr(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 1+1))) AS expected_or,
    arrayStringConcat(arrayMap(i -> char(bitXor(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 1+1))) AS expected_xor
SELECT 1, countIf(
    bitAnd(value, pattern) != expected_and OR
    bitAnd(pattern, value) != expected_and OR
    bitAnd(value, materialize(pattern)) != expected_and OR
    bitOr(value, pattern) != expected_or OR
    bitOr(pattern, value) != expected_or OR
    bitOr(value, materialize(pattern)) != expected_or OR
    bitXor(value, pattern) != expected_xor OR
    bitXor(pattern, value) != expected_xor OR
    bitXor(value, materialize(pattern)) != expected_xor)
FROM numbers(257);

WITH
    toFixedString(substring(repeat(concat(toString(number), unhex('00F17E')), 16), 1, 2), 2) AS value,
    toFixedString(substring(repeat(unhex('A501F0803C12D769'), 8), 1, 2), 2) AS pattern,
    arrayStringConcat(arrayMap(i -> char(bitAnd(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 2+1))) AS expected_and,
    arrayStringConcat(arrayMap(i -> char(bitOr(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 2+1))) AS expected_or,
    arrayStringConcat(arrayMap(i -> char(bitXor(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 2+1))) AS expected_xor
SELECT 2, countIf(
    bitAnd(value, pattern) != expected_and OR
    bitAnd(pattern, value) != expected_and OR
    bitAnd(value, materialize(pattern)) != expected_and OR
    bitOr(value, pattern) != expected_or OR
    bitOr(pattern, value) != expected_or OR
    bitOr(value, materialize(pattern)) != expected_or OR
    bitXor(value, pattern) != expected_xor OR
    bitXor(pattern, value) != expected_xor OR
    bitXor(value, materialize(pattern)) != expected_xor)
FROM numbers(257);

WITH
    toFixedString(substring(repeat(concat(toString(number), unhex('00F17E')), 16), 1, 3), 3) AS value,
    toFixedString(substring(repeat(unhex('A501F0803C12D769'), 8), 1, 3), 3) AS pattern,
    arrayStringConcat(arrayMap(i -> char(bitAnd(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 3+1))) AS expected_and,
    arrayStringConcat(arrayMap(i -> char(bitOr(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 3+1))) AS expected_or,
    arrayStringConcat(arrayMap(i -> char(bitXor(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 3+1))) AS expected_xor
SELECT 3, countIf(
    bitAnd(value, pattern) != expected_and OR
    bitAnd(pattern, value) != expected_and OR
    bitAnd(value, materialize(pattern)) != expected_and OR
    bitOr(value, pattern) != expected_or OR
    bitOr(pattern, value) != expected_or OR
    bitOr(value, materialize(pattern)) != expected_or OR
    bitXor(value, pattern) != expected_xor OR
    bitXor(pattern, value) != expected_xor OR
    bitXor(value, materialize(pattern)) != expected_xor)
FROM numbers(257);

WITH
    toFixedString(substring(repeat(concat(toString(number), unhex('00F17E')), 16), 1, 4), 4) AS value,
    toFixedString(substring(repeat(unhex('A501F0803C12D769'), 8), 1, 4), 4) AS pattern,
    arrayStringConcat(arrayMap(i -> char(bitAnd(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 4+1))) AS expected_and,
    arrayStringConcat(arrayMap(i -> char(bitOr(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 4+1))) AS expected_or,
    arrayStringConcat(arrayMap(i -> char(bitXor(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 4+1))) AS expected_xor
SELECT 4, countIf(
    bitAnd(value, pattern) != expected_and OR
    bitAnd(pattern, value) != expected_and OR
    bitAnd(value, materialize(pattern)) != expected_and OR
    bitOr(value, pattern) != expected_or OR
    bitOr(pattern, value) != expected_or OR
    bitOr(value, materialize(pattern)) != expected_or OR
    bitXor(value, pattern) != expected_xor OR
    bitXor(pattern, value) != expected_xor OR
    bitXor(value, materialize(pattern)) != expected_xor)
FROM numbers(257);

WITH
    toFixedString(substring(repeat(concat(toString(number), unhex('00F17E')), 16), 1, 6), 6) AS value,
    toFixedString(substring(repeat(unhex('A501F0803C12D769'), 8), 1, 6), 6) AS pattern,
    arrayStringConcat(arrayMap(i -> char(bitAnd(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 6+1))) AS expected_and,
    arrayStringConcat(arrayMap(i -> char(bitOr(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 6+1))) AS expected_or,
    arrayStringConcat(arrayMap(i -> char(bitXor(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 6+1))) AS expected_xor
SELECT 6, countIf(
    bitAnd(value, pattern) != expected_and OR
    bitAnd(pattern, value) != expected_and OR
    bitAnd(value, materialize(pattern)) != expected_and OR
    bitOr(value, pattern) != expected_or OR
    bitOr(pattern, value) != expected_or OR
    bitOr(value, materialize(pattern)) != expected_or OR
    bitXor(value, pattern) != expected_xor OR
    bitXor(pattern, value) != expected_xor OR
    bitXor(value, materialize(pattern)) != expected_xor)
FROM numbers(257);

WITH
    toFixedString(substring(repeat(concat(toString(number), unhex('00F17E')), 16), 1, 8), 8) AS value,
    toFixedString(substring(repeat(unhex('A501F0803C12D769'), 8), 1, 8), 8) AS pattern,
    arrayStringConcat(arrayMap(i -> char(bitAnd(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 8+1))) AS expected_and,
    arrayStringConcat(arrayMap(i -> char(bitOr(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 8+1))) AS expected_or,
    arrayStringConcat(arrayMap(i -> char(bitXor(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 8+1))) AS expected_xor
SELECT 8, countIf(
    bitAnd(value, pattern) != expected_and OR
    bitAnd(pattern, value) != expected_and OR
    bitAnd(value, materialize(pattern)) != expected_and OR
    bitOr(value, pattern) != expected_or OR
    bitOr(pattern, value) != expected_or OR
    bitOr(value, materialize(pattern)) != expected_or OR
    bitXor(value, pattern) != expected_xor OR
    bitXor(pattern, value) != expected_xor OR
    bitXor(value, materialize(pattern)) != expected_xor)
FROM numbers(257);

WITH
    toFixedString(substring(repeat(concat(toString(number), unhex('00F17E')), 16), 1, 15), 15) AS value,
    toFixedString(substring(repeat(unhex('A501F0803C12D769'), 8), 1, 15), 15) AS pattern,
    arrayStringConcat(arrayMap(i -> char(bitAnd(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 15+1))) AS expected_and,
    arrayStringConcat(arrayMap(i -> char(bitOr(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 15+1))) AS expected_or,
    arrayStringConcat(arrayMap(i -> char(bitXor(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 15+1))) AS expected_xor
SELECT 15, countIf(
    bitAnd(value, pattern) != expected_and OR
    bitAnd(pattern, value) != expected_and OR
    bitAnd(value, materialize(pattern)) != expected_and OR
    bitOr(value, pattern) != expected_or OR
    bitOr(pattern, value) != expected_or OR
    bitOr(value, materialize(pattern)) != expected_or OR
    bitXor(value, pattern) != expected_xor OR
    bitXor(pattern, value) != expected_xor OR
    bitXor(value, materialize(pattern)) != expected_xor)
FROM numbers(257);

WITH
    toFixedString(substring(repeat(concat(toString(number), unhex('00F17E')), 16), 1, 16), 16) AS value,
    toFixedString(substring(repeat(unhex('A501F0803C12D769'), 8), 1, 16), 16) AS pattern,
    arrayStringConcat(arrayMap(i -> char(bitAnd(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 16+1))) AS expected_and,
    arrayStringConcat(arrayMap(i -> char(bitOr(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 16+1))) AS expected_or,
    arrayStringConcat(arrayMap(i -> char(bitXor(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 16+1))) AS expected_xor
SELECT 16, countIf(
    bitAnd(value, pattern) != expected_and OR
    bitAnd(pattern, value) != expected_and OR
    bitAnd(value, materialize(pattern)) != expected_and OR
    bitOr(value, pattern) != expected_or OR
    bitOr(pattern, value) != expected_or OR
    bitOr(value, materialize(pattern)) != expected_or OR
    bitXor(value, pattern) != expected_xor OR
    bitXor(pattern, value) != expected_xor OR
    bitXor(value, materialize(pattern)) != expected_xor)
FROM numbers(257);

WITH
    toFixedString(substring(repeat(concat(toString(number), unhex('00F17E')), 16), 1, 17), 17) AS value,
    toFixedString(substring(repeat(unhex('A501F0803C12D769'), 8), 1, 17), 17) AS pattern,
    arrayStringConcat(arrayMap(i -> char(bitAnd(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 17+1))) AS expected_and,
    arrayStringConcat(arrayMap(i -> char(bitOr(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 17+1))) AS expected_or,
    arrayStringConcat(arrayMap(i -> char(bitXor(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 17+1))) AS expected_xor
SELECT 17, countIf(
    bitAnd(value, pattern) != expected_and OR
    bitAnd(pattern, value) != expected_and OR
    bitAnd(value, materialize(pattern)) != expected_and OR
    bitOr(value, pattern) != expected_or OR
    bitOr(pattern, value) != expected_or OR
    bitOr(value, materialize(pattern)) != expected_or OR
    bitXor(value, pattern) != expected_xor OR
    bitXor(pattern, value) != expected_xor OR
    bitXor(value, materialize(pattern)) != expected_xor)
FROM numbers(257);

WITH
    toFixedString(substring(repeat(concat(toString(number), unhex('00F17E')), 16), 1, 31), 31) AS value,
    toFixedString(substring(repeat(unhex('A501F0803C12D769'), 8), 1, 31), 31) AS pattern,
    arrayStringConcat(arrayMap(i -> char(bitAnd(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 31+1))) AS expected_and,
    arrayStringConcat(arrayMap(i -> char(bitOr(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 31+1))) AS expected_or,
    arrayStringConcat(arrayMap(i -> char(bitXor(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 31+1))) AS expected_xor
SELECT 31, countIf(
    bitAnd(value, pattern) != expected_and OR
    bitAnd(pattern, value) != expected_and OR
    bitAnd(value, materialize(pattern)) != expected_and OR
    bitOr(value, pattern) != expected_or OR
    bitOr(pattern, value) != expected_or OR
    bitOr(value, materialize(pattern)) != expected_or OR
    bitXor(value, pattern) != expected_xor OR
    bitXor(pattern, value) != expected_xor OR
    bitXor(value, materialize(pattern)) != expected_xor)
FROM numbers(257);

WITH
    toFixedString(substring(repeat(concat(toString(number), unhex('00F17E')), 16), 1, 64), 64) AS value,
    toFixedString(substring(repeat(unhex('A501F0803C12D769'), 8), 1, 64), 64) AS pattern,
    arrayStringConcat(arrayMap(i -> char(bitAnd(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 64+1))) AS expected_and,
    arrayStringConcat(arrayMap(i -> char(bitOr(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 64+1))) AS expected_or,
    arrayStringConcat(arrayMap(i -> char(bitXor(ascii(substring(value, i, 1)), ascii(substring(pattern, i, 1)))), range(1, 64+1))) AS expected_xor
SELECT 64, countIf(
    bitAnd(value, pattern) != expected_and OR
    bitAnd(pattern, value) != expected_and OR
    bitAnd(value, materialize(pattern)) != expected_and OR
    bitOr(value, pattern) != expected_or OR
    bitOr(pattern, value) != expected_or OR
    bitOr(value, materialize(pattern)) != expected_or OR
    bitXor(value, pattern) != expected_xor OR
    bitXor(pattern, value) != expected_xor OR
    bitXor(value, materialize(pattern)) != expected_xor)
FROM numbers(257);
