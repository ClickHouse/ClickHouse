-- The final `DISTINCT` inserts into its two-level hash set in parallel, bucket by bucket, once the set
-- holds more than 1000000 keys and a block has more than 10000 rows. Exercise that path with complex
-- keys (which select the `hashed_two_level` method), a single merged final stream and duplicates
-- spread across many blocks, and check the exact result.

SET max_threads = 4, max_block_size = 65536;
-- Keep a single final stream, so the whole thread budget goes to the pool of one transform.
SET allow_parallel_distinct = 0;
-- The parallel path is not used by external `DISTINCT`.
SET max_bytes_before_external_distinct = 0, max_bytes_ratio_before_external_distinct = 0, enable_adaptive_memory_spill_scheduler = 0;
SET max_rows_in_distinct = 0, max_bytes_in_distinct = 0;

SELECT count(), sum(n), sum(length(s))
FROM
(
    SELECT DISTINCT toString(number % 1500000) AS s, number % 1500000 AS n
    FROM numbers_mt(3000000)
);

-- The same keys with the order of the second half reversed, so that duplicates of the first half
-- arrive in different blocks than their originals.
SELECT count(), sum(n), sum(length(s))
FROM
(
    SELECT DISTINCT toString(k) AS s, k AS n
    FROM (SELECT if(number < 1500000, number, toUInt64(2999999 - number)) AS k FROM numbers_mt(3000000))
);
