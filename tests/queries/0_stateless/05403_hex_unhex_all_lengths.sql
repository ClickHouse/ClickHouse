-- `hex` and `unhex` of every length from 0 to 300 bytes, which covers each short-input path and the block tails of the
-- SIMD implementation that the server's CPU dispatches to. Every query counts mismatches against a reference and
-- prints 0, except the query that pins the deterministic decode of short non-hex input.

-- `hex` encodes the whole column at once, so one row per block makes it encode each length on its own. The rows come
-- straight from `numbers`, which respects `max_block_size`; a sort or a table read could merge them into one block.
SELECT countIf(hex(s) != arrayStringConcat(arrayMap(
        b -> concat(substring('0123456789ABCDEF', intDiv(b, 16) + 1, 1), substring('0123456789ABCDEF', b % 16 + 1, 1)),
        arrayMap(i -> reinterpretAsUInt8(substring(s, i, 1)), range(1, length(s) + 1)))))
FROM (SELECT arrayStringConcat(arrayMap(i -> char((number * 131 + i * 37 + 13) % 256), range(number % 301))) AS s FROM numbers(903))
SETTINGS max_block_size = 1;

DROP TABLE IF EXISTS t_hex_lengths;
CREATE TABLE t_hex_lengths (s String) ENGINE = Memory;
INSERT INTO t_hex_lengths SELECT arrayStringConcat(arrayMap(i -> char((number * 131 + i * 37 + 13) % 256), range(number % 301)))
FROM numbers(903);

-- `unhex` decodes row by row, so a single block covers every length.
SELECT countIf(unhex(hex(s)) != s) FROM t_hex_lengths;
SELECT countIf(unhex(lower(hex(s))) != s) FROM t_hex_lengths;

-- Every third digit lower-case, which falls on both the high and the low digit of a byte.
SELECT countIf(unhex(arrayStringConcat(arrayMap((c, i) -> if(i % 3 = 0, lower(c), c), splitByString('', hex(s)) AS digits, arrayEnumerate(digits)))) != s)
FROM t_hex_lengths;

-- Odd length: the leading digit becomes a byte on its own, followed by the pairs.
SELECT countIf(unhex(substring(hex(s), 2)) != concat(char(reinterpretAsUInt8(s) % 16), substring(s, 2)))
FROM t_hex_lengths
WHERE length(s) > 0;

-- `hex` over the whole table: one block holding rows of every length, so the encoder accumulates the per-row
-- offsets within a block, not just for a single row.
SELECT countIf(hex(s) != arrayStringConcat(arrayMap(
        b -> concat(substring('0123456789ABCDEF', intDiv(b, 16) + 1, 1), substring('0123456789ABCDEF', b % 16 + 1, 1)),
        arrayMap(i -> reinterpretAsUInt8(substring(s, i, 1)), range(1, length(s) + 1)))))
FROM t_hex_lengths;

-- Odd length with a non-hex leading digit: the leading digit decodes alone through the scalar table, which maps
-- invalid characters to 0xff, and the remaining pairs decode normally.
SELECT countIf(unhex(concat('z', hex(s))) != concat(char(255), s)) FROM t_hex_lengths;

-- Non-hex input up to 6 characters stays below the SIMD thresholds of every backend, so the scalar table path
-- decodes it on all machines: a byte with an invalid digit decodes to 0xff, and the first digit of an odd input
-- decodes alone. Longer non-hex input decodes to unspecified bytes that differ between the backends, so its bytes
-- are not pinned, but its length is still fixed by the input length.
SELECT hex(unhex('zz')), hex(unhex('z0')), hex(unhex('0z')), hex(unhex('zzz')), hex(unhex('zzzzzz'));
SELECT countIf(length(unhex(repeat('g', number % 300))) != intDiv(number % 300 + 1, 2)) FROM numbers(1000);

DROP TABLE t_hex_lengths;
