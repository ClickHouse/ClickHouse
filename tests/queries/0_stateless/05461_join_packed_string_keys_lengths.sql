-- The joins with packed `String` keys build the keys of a block, or of 256 rows, with `PackedStringJoinKey::buildRange`.
-- On aarch64 and on x86 with AVX2 it hashes long strings 8 at a time. It sorts them by length first when the strings
-- hashed in one vector would otherwise differ too much in length. It measures the lengths in 8-byte words, capped at 64
-- words (512 bytes). When 8 or more of 256 rows have 512 bytes or more, it compares their real lengths.
-- These joins must give the results of the same joins without packed keys, for keys of 0 to 5996 bytes: keys of all
-- lengths up to 300 bytes, long keys of 2 words, long keys of 25 words, only short keys, short and long keys
-- alternating, long keys alternating between 2 words and 36 or 37 words, long keys of all lengths up to 1115 bytes, and
-- keys of 521 to 619 bytes mixed with keys of 4000 to 5996 bytes. A left key is a right key, or a right key with one
-- byte changed at its start, in its middle or at its end, or with one byte more or less. 05457 and 05460 check that the
-- packed keys are used.
--
-- The right table is larger than the L2 cache, so that the `hash` join prefetches and builds the keys of its left blocks
-- 256 rows at a time.

CREATE TABLE keys (j UInt64, k String) ENGINE = Memory;
INSERT INTO keys
SELECT number, substring(repeat(hex(sipHash128(number)), intDiv(len, 32) + 1), 1, len)
FROM
(
    SELECT number, multiIf(
            number < 30000, number % 301,
            number < 60000, 16 + number % 8,
            number < 80000, 200 + number % 3,
            number < 90000, number % 16,
            number < 100000, if(number % 2 = 0, number % 16, 16 + number % 285),
            number < 120000, if(number % 2 = 0, 300 - number % 7, 16 + number % 5),
            number < 140000, 16 + number % 1100,
            if(number % 4 = 0, 4000 + number % 4000, 520 + number % 100)) AS len
    FROM numbers(142000)
);

-- `b` and `p` take only one of the empty keys of `keys`, so that the empty keys do not multiply the result.
CREATE TABLE b (k String, j UInt64) ENGINE = Memory;
CREATE TABLE p (k String, i UInt64) ENGINE = Memory;
INSERT INTO b SELECT k, j FROM keys WHERE k != '' OR j = 0;
INSERT INTO p
SELECT arrayJoin([
        k,
        concat('g', substring(k, 2)),
        concat(substring(k, 1, intDiv(length(k), 2)), 'g', substring(k, intDiv(length(k), 2) + 2)),
        concat(substring(k, 1, length(k) - 1), 'g'),
        concat(k, 'g'),
        substring(k, 1, length(k) - 1)]),
    j
FROM keys
WHERE k != '' OR j = 0;

SELECT 'parallel_hash', getSetting('enable_packed_string_keys_in_hash_join'), count(), sum(cityHash64(p.k, p.i, b.j))
FROM p INNER JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'parallel_hash', hash_join_layout = 'parallel', max_threads = 4,
    enable_packed_string_keys_in_hash_join = 0;
SELECT 'parallel_hash', getSetting('enable_packed_string_keys_in_hash_join'), count(), sum(cityHash64(p.k, p.i, b.j))
FROM p INNER JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'parallel_hash', hash_join_layout = 'parallel', max_threads = 4,
    enable_packed_string_keys_in_hash_join = 1;

SELECT 'hash', getSetting('enable_packed_string_keys_in_hash_join'), count(), sum(cityHash64(p.k, p.i, b.j))
FROM p INNER JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_packed_string_keys_in_hash_join = 0;
SELECT 'hash', getSetting('enable_packed_string_keys_in_hash_join'), count(), sum(cityHash64(p.k, p.i, b.j))
FROM p INNER JOIN b ON p.k = b.k
SETTINGS join_algorithm = 'hash', max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_packed_string_keys_in_hash_join = 1;
