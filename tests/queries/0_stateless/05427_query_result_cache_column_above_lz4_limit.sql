-- A column bigger than `LZ4_MAX_INPUT_SIZE` (about 1.97 GiB) must stay uncompressed instead of failing with `CANNOT_COMPRESS`.

-- The query result cache compresses the result after the query finishes.
SELECT materialize(repeat('x', 1000000)) FROM numbers(2150) FORMAT Null SETTINGS use_query_cache = 1, query_cache_squash_partial_results = 0, query_cache_compress_entries = 1, max_result_bytes = 0;
SELECT 'query result cache: ok';

-- The `Memory` engine with `compress = 1` compresses every inserted block.
DROP TABLE IF EXISTS t_memory_lz4_limit;
CREATE TABLE t_memory_lz4_limit (s String) ENGINE = Memory SETTINGS compress = 1;
INSERT INTO t_memory_lz4_limit SELECT repeat('x', 1000000) FROM numbers(2150);
SELECT count(), sum(length(s)) FROM t_memory_lz4_limit;
DROP TABLE t_memory_lz4_limit;
