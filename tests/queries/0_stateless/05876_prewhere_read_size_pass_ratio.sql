-- A read filtered by PREWHERE is sized so that the filtered block approaches `preferred_block_size_bytes`.
-- Each query also checks that its result equals the one of the same filter in WHERE.

SET max_threads = 1, max_block_size = 65409, preferred_block_size_bytes = 1000000, preferred_max_column_in_block_size_bytes = 0,
    use_query_condition_cache = 0, use_columns_cache = 0, enable_parallel_replicas = 0, enable_multiple_prewhere_read_steps = 1,
    allow_prefetched_read_pool_for_local_filesystem = 0, allow_prefetched_read_pool_for_remote_filesystem = 0;

DROP TABLE IF EXISTS t_pass_ratio_uniform;
DROP TABLE IF EXISTS t_pass_ratio_runs;
DROP TABLE IF EXISTS t_pass_ratio_wide;

-- One row in 8 passes, and the rows that pass are spread evenly: a read takes several granules.
CREATE TABLE t_pass_ratio_uniform (u UInt64, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, use_const_adaptive_granularity = 0, enable_block_number_column = 0,
    enable_block_offset_column = 0, string_serialization_version = 'single_stream';
INSERT INTO t_pass_ratio_uniform SELECT number, if(number % 8 = 0, repeat('x', 100), '') FROM numbers(1000000);
OPTIMIZE TABLE t_pass_ratio_uniform FINAL;

SELECT 'uniform', round(sum(1 / bs)) < 70,
    sum(h) = (SELECT sum(cityHash64(s, u)) FROM t_pass_ratio_uniform WHERE s != '' SETTINGS optimize_move_to_prewhere = 0)
FROM (SELECT blockSize() AS bs, cityHash64(s, u) AS h FROM t_pass_ratio_uniform PREWHERE s != '');

-- The rows that pass come in runs of 32 granules after 48 granules without them: the first read of a run stays bounded.
CREATE TABLE t_pass_ratio_runs (id UInt64, f UInt8, s1 String, s2 String) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, use_const_adaptive_granularity = 0, enable_block_number_column = 0,
    enable_block_offset_column = 0, string_serialization_version = 'single_stream';
INSERT INTO t_pass_ratio_runs SELECT number, (number % 81920) >= 49152, leftPad(toString(number), 48, 'a'), leftPad(toString(number), 48, '-')
FROM numbers(327680);
OPTIMIZE TABLE t_pass_ratio_runs FINAL;

SELECT 'runs', max(bs) <= 8 * 2048,
    sum(h) = (SELECT sum(cityHash64(s1, s2)) FROM t_pass_ratio_runs WHERE f SETTINGS optimize_move_to_prewhere = 0)
FROM (SELECT blockSize() AS bs, cityHash64(s1, s2) AS h FROM t_pass_ratio_runs PREWHERE f)
SETTINGS preferred_block_size_bytes = 125000;

-- The column of PREWHERE is wide and is not read later: the read stays within the preferred size.
-- `substring` reads the whole column, unlike `startsWith`.
CREATE TABLE t_pass_ratio_wide (k UInt32, w String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1, use_const_adaptive_granularity = 0, enable_block_number_column = 0,
    enable_block_offset_column = 0, string_serialization_version = 'single_stream';
INSERT INTO t_pass_ratio_wide SELECT number, concat(if(number % 8 = 0, 'a', 'b'), repeat('z', 99)) FROM numbers(1000000);
OPTIMIZE TABLE t_pass_ratio_wide FINAL;

SELECT 'wide prewhere column', round(sum(1 / bs)) >= 110,
    sum(k) = (SELECT sum(k) FROM t_pass_ratio_wide WHERE substring(w, 1, 1) = 'a' SETTINGS optimize_move_to_prewhere = 0)
FROM (SELECT blockSize() AS bs, k FROM t_pass_ratio_wide PREWHERE substring(w, 1, 1) = 'a');

DROP TABLE t_pass_ratio_uniform;
DROP TABLE t_pass_ratio_runs;
DROP TABLE t_pass_ratio_wide;
