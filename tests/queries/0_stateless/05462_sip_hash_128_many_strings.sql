-- `sipHash128` hashes the strings of a `String` or `FixedString` column several at a time. `sipHash128Keyed` with zero
-- keys computes the same hashes one string at a time. The checksums in the reference were computed by the code that
-- hashes one string at a time, so they must not change.

DROP TABLE IF EXISTS sip_hash_strings;

CREATE TABLE sip_hash_strings
(
    s String,
    f FixedString(5),
    n Nullable(String),
    lc LowCardinality(String),
    a Array(String),
    m Map(String, String),
    t Tuple(String, FixedString(5))
)
ENGINE = Memory;

-- Every length from 0 to 300 several times, then strings of about 4 KB and short strings mixed. Strings are hashed 8 at
-- a time, so 3 of the 3003 rows are hashed one by one.
INSERT INTO sip_hash_strings
SELECT
    substring(repeat(hex(cityHash64(number)), 300), 1, len) AS str,
    toFixedString(substring(str, 1, 5), 5),
    if(number % 7 = 0, NULL, str),
    str,
    [str, substring(str, 2), ''],
    map(str, substring(str, 3)),
    (str, toFixedString(substring(str, 2, 5), 5))
FROM
(
    SELECT number, multiIf(number < 2000, number % 301, number % 3 = 0, 4000 + number % 9, number % 64) AS len
    FROM numbers(3003)
);

SELECT 'String', countIf(sipHash128(s) != sipHash128Keyed((0::UInt64, 0::UInt64), s)), groupBitXor(cityHash64(sipHash128(s))) FROM sip_hash_strings;
SELECT 'FixedString', countIf(sipHash128(f) != sipHash128Keyed((0::UInt64, 0::UInt64), f)), groupBitXor(cityHash64(sipHash128(f))) FROM sip_hash_strings;
SELECT 'Nullable', countIf(NOT isNotDistinctFrom(sipHash128(n), sipHash128Keyed((0::UInt64, 0::UInt64), n))), groupBitXor(cityHash64(sipHash128(n))) FROM sip_hash_strings;
SELECT 'LowCardinality', countIf(sipHash128(lc) != sipHash128Keyed((0::UInt64, 0::UInt64), lc)), groupBitXor(cityHash64(sipHash128(lc))) FROM sip_hash_strings;
SELECT 'Array', countIf(sipHash128(a) != sipHash128Keyed((0::UInt64, 0::UInt64), a)), groupBitXor(cityHash64(sipHash128(a))) FROM sip_hash_strings;
SELECT 'Map', countIf(sipHash128(m) != sipHash128Keyed((0::UInt64, 0::UInt64), m)), groupBitXor(cityHash64(sipHash128(m))) FROM sip_hash_strings;
SELECT 'Tuple', countIf(sipHash128(t) != sipHash128Keyed((0::UInt64, 0::UInt64), t)), groupBitXor(cityHash64(sipHash128(t))) FROM sip_hash_strings;
SELECT 'Several arguments', countIf(sipHash128(s, f, s) != sipHash128Keyed((0::UInt64, 0::UInt64), s, f, s)), groupBitXor(cityHash64(sipHash128(s, f, s))) FROM sip_hash_strings;

-- `Memory` returns the blocks as they were inserted, so these queries make their strings from `numbers`. Blocks of 7 rows
-- are smaller than one group of 8 strings hashed together, and blocks of 257 rows end with a chunk of one row.
SELECT 'Block of 7', countIf(sipHash128(s) != sipHash128Keyed((0::UInt64, 0::UInt64), s))
FROM (SELECT substring(repeat(hex(cityHash64(number)), 300), 1, number % 301) AS s FROM numbers(3003)) SETTINGS max_block_size = 7;
SELECT 'Block of 257', countIf(sipHash128(s) != sipHash128Keyed((0::UInt64, 0::UInt64), s))
FROM (SELECT substring(repeat(hex(cityHash64(number)), 300), 1, number % 301) AS s FROM numbers(3003)) SETTINGS max_block_size = 257;
SELECT 'FixedString, block of 7', countIf(sipHash128(f) != sipHash128Keyed((0::UInt64, 0::UInt64), f))
FROM (SELECT toFixedString(hex(cityHash64(number)), 16) AS f FROM numbers(3003)) SETTINGS max_block_size = 7;

DROP TABLE sip_hash_strings;
