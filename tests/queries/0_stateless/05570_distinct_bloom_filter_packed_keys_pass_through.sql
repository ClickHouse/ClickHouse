-- The preliminary `DISTINCT` with the bloom filter switches to pass-through when the projected memory
-- does not fit into the remaining `max_bytes_before_external_distinct` budget. The projection includes
-- the packed keys of the hash method: a `Nullable(UInt64)` key is packed into a 16-byte key, larger than
-- its 9 bytes per row in the block. Whether the preliminary sets are kept or released, the final result
-- must stay exact.

-- The bloom filter is only used when no exact row limit is configured, and the test profile of the
-- CI sets `max_rows_in_distinct` to a large value, so it has to be reset explicitly here.
SET max_rows_in_distinct = 0, max_bytes_in_distinct = 0;
SET distinct_set_limit_for_enabling_bloom_filter = 1000;
SET max_threads = 4, max_block_size = 8192;

SELECT count(), countIf(k IS NULL), sum(k)
FROM
(
    SELECT DISTINCT if(number % 10 = 0, NULL, number % 200000)::Nullable(UInt64) AS k
    FROM numbers_mt(1000000)
)
SETTINGS max_bytes_before_external_distinct = '1M';

SELECT count(), countIf(k IS NULL), sum(k)
FROM
(
    SELECT DISTINCT if(number % 10 = 0, NULL, number % 200000)::Nullable(UInt64) AS k
    FROM numbers_mt(1000000)
)
SETTINGS max_bytes_before_external_distinct = 0;
