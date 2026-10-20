-- `CASE` with constant `WHEN` keys compares them with the expression like `equals`, which compares a `FixedString`
-- with a `String` zero-padded, and its constant `THEN` values follow `cast_fixed_string_to_string_strip_trailing_zeros`,
-- the same as with non-constant ones.

SELECT
    'a' = toFixedString('a', 2),
    CASE 'a' WHEN toFixedString('a', 2) THEN 1 WHEN 'b' THEN 2 ELSE 0 END,
    CASE 'a' WHEN materialize(toFixedString('a', 2)) THEN 1 WHEN 'b' THEN 2 ELSE 0 END,
    CASE 'b' WHEN toFixedString('a', 2) THEN 1 WHEN 'b' THEN 2 ELSE 0 END,
    CASE toFixedString('b', 3) WHEN 'a' THEN 1 WHEN toFixedString('b', 2) THEN 2 ELSE 0 END;

SELECT number, CASE toString(number) WHEN toFixedString('0', 2) THEN 'zero' WHEN '1' THEN 'one' WHEN toFixedString('2', 3) THEN 'two' ELSE 'other' END
FROM numbers(4);

SELECT
    hex(CASE number WHEN 0 THEN toFixedString('a', 2) ELSE 'x' END),
    hex(CASE number WHEN 0 THEN materialize(toFixedString('a', 2)) ELSE 'x' END)
FROM numbers(1)
SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 1;

SELECT
    hex(CASE number WHEN 0 THEN toFixedString('a', 2) ELSE 'x' END),
    hex(CASE number WHEN 0 THEN materialize(toFixedString('a', 2)) ELSE 'x' END)
FROM numbers(1)
SETTINGS cast_fixed_string_to_string_strip_trailing_zeros = 0;
