-- `enable_join_in_memory_compression` must reduce the footprint of `Nullable` right-side payloads as well:
-- `ColumnNullable::compress` used to report the size of the source column, so the compressed column was
-- never smaller than the original one and was discarded. The join below is limited by `max_bytes_in_join`
-- to a fraction of the uncompressed payload, so it succeeds only if the payload is actually kept compressed.

DROP TABLE IF EXISTS jimc_n_left;
DROP TABLE IF EXISTS jimc_n_right;

CREATE TABLE jimc_n_left (k UInt64) ENGINE = Memory;
CREATE TABLE jimc_n_right (k UInt64, pad String, npad Nullable(String)) ENGINE = Memory;

INSERT INTO jimc_n_left SELECT number FROM numbers(10000);
-- About 10 MB uncompressed per payload column, in many blocks.
INSERT INTO jimc_n_right SELECT number, repeat('x', 1000), if(number % 10 = 0, NULL, repeat('y', 1000)) FROM numbers(10000)
SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000;

SET enable_analyzer = 1, join_algorithm = 'hash', query_plan_join_swap_table = 'false',
    max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0,
    enable_join_in_memory_compression = 1, join_in_memory_compression_min_bytes = 0;

-- A `Nullable(String)` payload.
SELECT count(), countIf(r.npad IS NULL), sum(length(r.npad))
FROM jimc_n_left AS l INNER JOIN (SELECT k, npad FROM jimc_n_right) AS r ON l.k = r.k
SETTINGS max_bytes_in_join = '3Mi', join_overflow_mode = 'throw';

-- A `String` payload of a `LEFT JOIN` with `join_use_nulls = 1`: the stored right columns become `Nullable`.
SELECT count(), countIf(r.pad IS NULL), sum(length(r.pad))
FROM jimc_n_left AS l LEFT JOIN (SELECT k, pad FROM jimc_n_right) AS r ON l.k = r.k
SETTINGS join_use_nulls = 1, max_bytes_in_join = '3Mi', join_overflow_mode = 'throw';

DROP TABLE jimc_n_left;
DROP TABLE jimc_n_right;

-- `parallel_hash` with few keys: only the slots that received right-side rows compress them, while the
-- other slots stay empty. The slots share one index of stored blocks, so every slot must read it as
-- possibly compressed, even one that did not compress anything itself.
SELECT count(), sum(l.attr + r.attr)
FROM (SELECT number % 2 AS id, sipHash64(number, 1) % 2 AS attr FROM numbers(2000)) AS l
LEFT JOIN (SELECT number % 2 AS id, sipHash64(number, 2) % 2 AS attr FROM numbers(2000)) AS r
ON l.id = r.id AND l.attr != r.attr
SETTINGS join_algorithm = 'parallel_hash', max_threads = 8;
